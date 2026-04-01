"""clAWS plan tool — translate objective to concrete execution plan.

This is the ONLY tool that accepts a free-text objective. It returns a
concrete query, cost estimate, and output schema without executing anything.

LLM reasoning happens here. Bedrock Guardrails filters both input
(objective) and output (generated query).
"""

import json
import os
from typing import Any

from tools.plan.validators.cost_estimator import estimate_cost
from tools.plan.validators.sql_validator import validate_sql
from tools.shared import (
    GUARDRAIL_ID,
    GUARDRAIL_VERSION,
    audit_log,
    bedrock_runtime,
    error,
    get_cached_schema,
    new_plan_id,
    store_plan,
    success,
)

MODEL_ID = os.environ.get("CLAWS_PLAN_MODEL_ID", "anthropic.claude-sonnet-4-20250514-v1:0")


def handler(event: dict, context: Any) -> dict:
    """Lambda handler for claws.plan."""
    body = json.loads(event.get("body", "{}")) if isinstance(event.get("body"), str) else event
    objective = body.get("objective", "")
    source_id = body.get("source_id", "")
    constraints = body.get("constraints", {})
    principal = event.get("requestContext", {}).get("authorizer", {}).get("principalId", "unknown")
    request_id = event.get("requestContext", {}).get("requestId", "")

    if not objective:
        return error("objective is required")
    if not source_id:
        return error("source_id is required")

    # Load cached schema from a prior probe call
    schema = get_cached_schema(source_id)
    if schema is None:
        return error(
            f"No cached schema for {source_id}. Run claws.probe first.",
            status_code=422,
        )

    # Determine query type from source_id backend
    backend = source_id.split(":")[0]
    query_type = _backend_to_query_type(backend)

    # Build the prompt
    prompt = _build_plan_prompt(objective, source_id, schema, constraints, query_type)

    # Call Bedrock with guardrail attached
    invoke_kwargs = {
        "modelId": MODEL_ID,
        "body": json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 2048,
        }),
    }

    # Attach guardrail if configured
    guardrail_trace = None
    if GUARDRAIL_ID:
        invoke_kwargs["guardrailIdentifier"] = GUARDRAIL_ID
        invoke_kwargs["guardrailVersion"] = GUARDRAIL_VERSION

    try:
        response = bedrock_runtime().invoke_model(**invoke_kwargs)
        result = json.loads(response["body"].read())
    except Exception as e:
        audit_log(
            "plan", principal, body, {"status": "error", "error": str(e)}, request_id=request_id
        )
        return error(f"Model invocation failed: {e}", status_code=502)

    # Check if guardrail intervened
    if result.get("amazon-bedrock-guardrailAction") == "INTERVENED":
        guardrail_trace = result.get("amazon-bedrock-trace", {}).get("guardrail", {})
        audit_log("plan", principal, body, {
            "status": "blocked",
            "guardrail_trace": guardrail_trace,
        }, request_id=request_id)
        return success({
            "status": "blocked",
            "reason": "Content policy violation on objective or generated query",
        })

    # Parse the generated query from the model response
    model_text = ""
    for block in result.get("content", []):
        if block.get("type") == "text":
            model_text += block["text"]

    parsed = _parse_model_response(model_text)
    if parsed is None:
        audit_log("plan", principal, body, {
            "status": "rejected", "reason": "Failed to parse model response",
        }, request_id=request_id)
        return error("Failed to parse plan from model response", status_code=502)

    generated_query = parsed["query"]
    output_schema = parsed.get("output_schema", {})

    # Validate the generated query
    validation = validate_sql(generated_query, constraints)
    if not validation["ok"]:
        audit_log(
            "plan", principal, body,
            {"status": "rejected", "reason": validation["reason"]},
            request_id=request_id,
        )
        return success({
            "status": "rejected",
            "reason": validation["reason"],
        })

    # Estimate cost
    cost_est = estimate_cost(source_id, generated_query, schema)

    # Check cost against constraints
    max_cost = constraints.get("max_cost_dollars")
    if max_cost and cost_est["estimated_cost_dollars"] > max_cost:
        cost_msg = (
            f"Estimated cost ${cost_est['estimated_cost_dollars']:.2f}"
            f" exceeds limit ${max_cost:.2f}"
        )
        audit_log(
            "plan", principal, body, {"status": "rejected", "reason": cost_msg},
            request_id=request_id,
        )
        return success({
            "status": "rejected",
            "reason": cost_msg,
            "estimated_cost": f"${cost_est['estimated_cost_dollars']:.2f}",
        })

    # Store the plan
    plan_id = new_plan_id()
    plan = {
        "source_id": source_id,
        "query": generated_query,
        "query_type": query_type,
        "constraints": {
            "max_bytes_scanned": cost_est.get("estimated_bytes_scanned", 0),
            "timeout_seconds": constraints.get("timeout_seconds", 30),
            "read_only": constraints.get("read_only", True),
        },
    }
    store_plan(plan_id, plan)

    response_body = {
        "plan_id": plan_id,
        "status": "ready",
        "steps": [
            {
                "tool": "claws.excavate",
                "input": {
                    "plan_id": plan_id,
                    "source_id": source_id,
                    "query": generated_query,
                    "query_type": query_type,
                    "constraints": plan["constraints"],
                },
            }
        ],
        "estimated_cost": f"${cost_est['estimated_cost_dollars']:.2f}",
        "estimated_bytes_scanned": cost_est.get("estimated_bytes_scanned", 0),
        "output_schema": output_schema,
    }

    audit_log("plan", principal, body, {
        "status": "ready",
        "plan_id": plan_id,
        "estimated_cost": cost_est["estimated_cost_dollars"],
        "guardrail_trace": guardrail_trace,
    }, request_id=request_id)

    return success(response_body)


def _backend_to_query_type(backend: str) -> str:
    return {
        "athena": "athena_sql",
        "opensearch": "opensearch_dsl",
        "s3": "s3_select_sql",
        "dynamodb": "dynamodb_partiql",
    }.get(backend, "athena_sql")


def _build_plan_prompt(
    objective: str,
    source_id: str,
    schema: dict,
    constraints: dict,
    query_type: str,
) -> str:
    """Build the prompt for query generation."""
    from decimal import Decimal
    def _decimal_default(x: object) -> object:
        if isinstance(x, Decimal):
            return int(x) if x == int(x) else float(x)
        return str(x)

    schema_text = json.dumps(schema, indent=2, default=_decimal_default)
    constraints_text = json.dumps(constraints, indent=2)

    return f"""You are a query planner for the clAWS excavation system. Your job is to
translate a natural-language objective into a concrete, executable query.

SOURCE: {source_id}
QUERY TYPE: {query_type}

SCHEMA:
{schema_text}

CONSTRAINTS:
{constraints_text}

OBJECTIVE: {objective}

Rules:
1. Generate ONLY a read-only query. No INSERT, UPDATE, DELETE, DROP, ALTER, CREATE.
2. Respect constraints — especially max_bytes_scanned and timeout.
3. Use partition keys in WHERE clauses when available to reduce scan cost.
4. Return results in a shape that directly answers the objective.

Respond in JSON with exactly this structure:
{{
  "query": "<the concrete SQL/DSL query>",
  "output_schema": {{
    "columns": ["col1", "col2"],
    "estimated_rows": <number>
  }},
  "reasoning": "<brief explanation of query design choices>"
}}

Respond ONLY with the JSON object. No markdown, no backticks, no preamble."""


def _parse_model_response(text: str) -> dict | None:
    """Parse the JSON response from the model."""
    # Strip any accidental markdown fencing
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
    if text.endswith("```"):
        text = text[:-3]
    text = text.strip()

    try:
        parsed: dict = json.loads(text)
        return parsed
    except json.JSONDecodeError:
        # Try to extract JSON from the response
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            try:
                extracted: dict = json.loads(text[start:end])
                return extracted
            except json.JSONDecodeError:
                pass
    return None
