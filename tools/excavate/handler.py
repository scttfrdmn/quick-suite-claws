"""clAWS excavate tool — execute a concrete query from a plan.

No free-text objectives. Takes exactly what the plan tool produced.
If plan_id is provided, validates the query matches the stored plan.
Results are scanned via ApplyGuardrail before return to agent.
"""

import json
from typing import Any

from tools.errors import ForbiddenError, NotFoundError
from tools.excavate.executors.athena import execute_athena
from tools.excavate.executors.dynamodb import execute_dynamodb
from tools.excavate.executors.mcp import execute_mcp
from tools.excavate.executors.opensearch import execute_opensearch
from tools.excavate.executors.s3_select import execute_s3_select
from tools.shared import (
    audit_log,
    error,
    load_plan,
    new_run_id,
    scan_payload,
    store_result,
    store_result_metadata,
    success,
    validate_source_id,
)


def _infer_schema(rows: list[dict]) -> list[dict]:
    """Infer column schema from first result row. Returns [] if rows is empty."""
    if not rows:
        return []
    schema = []
    for name, value in rows[0].items():
        if isinstance(value, bool):
            col_type = "boolean"
        elif isinstance(value, int):
            col_type = "bigint"
        elif isinstance(value, float):
            col_type = "double"
        else:
            col_type = "string"
        schema.append({"name": name, "type": col_type})
    return schema


EXECUTORS = {
    "athena_sql": execute_athena,
    "dynamodb_partiql": execute_dynamodb,
    "opensearch_dsl": execute_opensearch,
    "s3_select_sql": execute_s3_select,
    "mcp_tool": execute_mcp,
}


def handler(event: dict, context: Any) -> dict:
    """Lambda handler for claws.excavate."""
    body = json.loads(event.get("body", "{}")) if isinstance(event.get("body"), str) else event
    plan_id = body.get("plan_id", "")
    source_id = body.get("source_id", "")
    query = body.get("query", "")
    query_type = body.get("query_type", "")
    constraints = body.get("constraints", {})
    principal = event.get("requestContext", {}).get("authorizer", {}).get("principalId", "unknown")
    request_id = event.get("requestContext", {}).get("requestId", "")

    if not source_id or not query or not query_type:
        return error("source_id, query, and query_type are required")

    try:
        validate_source_id(source_id)
    except ValueError as exc:
        return error(str(exc))

    # Validate against stored plan if plan_id is provided
    loaded_plan: dict | None = None
    if plan_id:
        plan = load_plan(plan_id)
        loaded_plan = plan
        if plan is None:
            return error(NotFoundError(f"Plan {plan_id} not found"))

        # Block execution if the plan requires IRB approval and is not yet approved
        plan_status = plan.get("status", "ready")
        if plan_status == "pending_approval":
            audit_log("excavate", principal, body, {
                "status": "pending_approval",
                "plan_id": plan_id,
                "reason": "Plan requires IRB approval before execution",
            }, request_id=request_id)
            return success({
                "status": "pending_approval",
                "plan_id": plan_id,
                "message": "This plan requires IRB approval before execution",
            })

        # Check principal is authorized: must be plan owner OR in shared_with list
        plan_owner = plan.get("created_by", "")
        shared_with = plan.get("shared_with", [])
        if plan_owner and principal != plan_owner and principal not in shared_with:
            audit_log("excavate", principal, body, {
                "status": "rejected",
                "reason": "Principal not authorized to excavate this plan",
            }, request_id=request_id)
            return error(ForbiddenError(
                "Not authorized to excavate this plan. "
                "You must be the plan owner or have been granted access via share_plan."
            ))

        # Verify the query matches the plan — prevents bait-and-switch
        if plan.get("query") != query:
            audit_log("excavate", principal, body, {
                "status": "rejected",
                "reason": "Query does not match stored plan",
            }, request_id=request_id)
            return error(ForbiddenError(
                "Query does not match stored plan. Submit the exact query from the plan."
            ))

    # Get executor for this query type
    executor = EXECUTORS.get(query_type)
    if executor is None:
        return error(f"Unsupported query_type: {query_type}")

    # Execute the query
    run_id = new_run_id()

    try:
        exec_result = executor(
            source_id=source_id,
            query=query,
            constraints=constraints,
            run_id=run_id,
        )
    except Exception as e:
        audit_log("excavate", principal, body, {
            "status": "error",
            "run_id": run_id,
            "error": str(e),
        }, request_id=request_id)
        return error(f"Execution failed: {e}", status_code=500)

    if exec_result.get("status") == "error":
        audit_log("excavate", principal, body, {
            "status": "error",
            "run_id": run_id,
            "error": exec_result.get("error"),
        }, request_id=request_id)
        return error(exec_result["error"], status_code=500)

    # Column-level post-filter: strip any columns not in the plan's allowed_columns list.
    # Defence-in-depth — even if the LLM generated SQL referencing a restricted column
    # (which it should not, since probe only showed it public columns), the result is
    # stripped before guardrails scan and before S3 write.
    if loaded_plan is not None and exec_result.get("rows"):
        allowed = loaded_plan.get("allowed_columns")
        if allowed is not None:
            allowed_set = set(allowed)
            exec_result["rows"] = [
                {k: v for k, v in row.items() if k in allowed_set}
                for row in exec_result["rows"]
            ]

    # Scan results for PII/PHI via ApplyGuardrail before returning
    if exec_result.get("rows"):
        scan_result = scan_payload(exec_result["rows"])
        if scan_result["status"] == "blocked":
            # Store raw results (for audit) but don't return them
            store_result(run_id, exec_result["rows"])
            audit_log("excavate", principal, body, {
                "status": "blocked",
                "run_id": run_id,
                "reason": "Results contain sensitive content",
            }, request_id=request_id)
            return success({
                "run_id": run_id,
                "status": "blocked",
                "reason": "Results contain sensitive content detected by guardrail",
                "rows_returned": 0,
                "bytes_scanned": exec_result.get("bytes_scanned", 0),
                "cost": exec_result.get("cost", "$0.00"),
            })

    # Store results in S3
    rows = exec_result.get("rows", [])
    result_uri = store_result(run_id, rows)

    # Write companion metadata file for downstream consumers (e.g. Compute)
    metadata_uri = store_result_metadata(
        run_id=run_id,
        schema=_infer_schema(rows),
        row_count=len(rows),
        bytes_scanned=exec_result.get("bytes_scanned", 0),
        cost=exec_result.get("cost", "$0.00"),
        source_id=source_id,
    )

    # Build preview (first 5 rows)
    preview = rows[:5] if rows else []

    response_body = {
        "run_id": run_id,
        "status": "complete",
        "rows_returned": len(rows),
        "bytes_scanned": exec_result.get("bytes_scanned", 0),
        "cost": exec_result.get("cost", "$0.00"),
        "result_uri": result_uri,
        "metadata_uri": metadata_uri,
        "result_preview": preview,
    }

    audit_log("excavate", principal, body, {
        "status": "complete",
        "run_id": run_id,
        "rows_returned": len(rows),
        "bytes_scanned": exec_result.get("bytes_scanned", 0),
        "cost": exec_result.get("cost"),
    }, request_id=request_id)

    return success(response_body)
