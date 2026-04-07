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
    call_router,
    error,
    get_cached_schema,
    new_plan_id,
    store_plan,
    success,
    validate_source_id,
)

MODEL_ID = os.environ.get("CLAWS_PLAN_MODEL_ID", "anthropic.claude-sonnet-4-20250514-v1:0")


def handler(event: dict, context: Any) -> dict:
    """Lambda handler for claws.plan."""
    body = json.loads(event.get("body", "{}")) if isinstance(event.get("body"), str) else event
    objective = body.get("objective", "")
    source_id = body.get("source_id", "")
    constraints = body.get("constraints", {})
    team_id = body.get("team_id")
    authorizer = event.get("requestContext", {}).get("authorizer", {})
    principal = authorizer.get("principalId", "unknown")
    # Principal roles are set by Cognito user pool groups and injected by API Gateway
    # authorizer. Used to determine column-level visibility access.
    principal_roles: list[str] = json.loads(authorizer.get("roles", "[]"))
    request_id = event.get("requestContext", {}).get("requestId", "")

    if not objective:
        return error("objective is required")
    if not source_id:
        return error("source_id is required")

    try:
        validate_source_id(source_id)
    except ValueError as exc:
        return error(str(exc))

    # Determine query type from source_id backend (needed for early validation)
    backend = source_id.split(":")[0]
    query_type = _backend_to_query_type(backend)

    # Validate that MCP source_ids reference a registered server before any schema lookup.
    # The executor resolves the actual endpoint from the registry — never from user input —
    # so an unregistered server name would silently fail at execution time. Catching it here
    # gives a clear error and prevents plans with invalid source_ids from being stored.
    if query_type == "mcp_tool":
        from tools.mcp.registry import known_servers  # noqa: PLC0415
        server_name = source_id[6:].split("/")[0]  # strip "mcp://"
        if server_name not in known_servers():
            return error(
                f"MCP server '{server_name}' is not registered in the MCP registry. "
                "Verify CLAWS_MCP_SERVERS_CONFIG includes this server.",
                status_code=422,
            )

    # Load cached schema from a prior probe call
    schema = get_cached_schema(source_id)
    if schema is None:
        return error(
            f"No cached schema for {source_id}. Run claws.probe first.",
            status_code=422,
        )

    # Filter schema columns to those visible to this principal.
    # Columns tagged visibility=phi require the "phi_cleared" role.
    # Columns tagged visibility=restricted require the "pii_access" role.
    # Columns tagged visibility=public (or untagged) are always visible.
    # The filtered schema is passed to the LLM — it cannot generate queries
    # referencing columns it was never shown.
    schema, allowed_columns = _filter_schema_columns(schema, principal_roles)

    # Build the prompt
    prompt = _build_plan_prompt(objective, source_id, schema, constraints, query_type)

    # Router-first: delegate to Quick Suite model router if configured.
    # Falls back to direct Bedrock when ROUTER_ENDPOINT is not set or on any error.
    guardrail_trace = None
    model_text = call_router("generate", prompt, max_tokens=2048)

    if model_text is None:
        # Direct Bedrock invocation with attached guardrail
        invoke_kwargs: dict = {
            "modelId": MODEL_ID,
            "body": json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 2048,
            }),
        }
        if GUARDRAIL_ID:
            invoke_kwargs["guardrailIdentifier"] = GUARDRAIL_ID
            invoke_kwargs["guardrailVersion"] = GUARDRAIL_VERSION

        try:
            response = bedrock_runtime().invoke_model(**invoke_kwargs)
            result = json.loads(response["body"].read())
        except Exception as e:
            audit_log(
                "plan", principal, body, {"status": "error", "error": str(e)},
                request_id=request_id,
            )
            return error(f"Model invocation failed: {e}", status_code=502)

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

    # MCP queries bypass SQL validation and cost estimation
    if query_type == "mcp_tool":
        cost_est = {"estimated_bytes_scanned": 0, "estimated_cost_dollars": 0.0}
    else:
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
    # requires_irb may be passed in by the caller or set on the source's policy
    # context. When true, the plan is stored with status "pending_approval" and
    # excavate will block until an IRB approver calls approve_plan.
    requires_irb = body.get("requires_irb", False)
    plan_status = "pending_approval" if requires_irb else "ready"

    plan_id = new_plan_id()
    plan = {
        "source_id": source_id,
        "query": generated_query,
        "query_type": query_type,
        "created_by": principal,
        "status": plan_status,
        "constraints": {
            "max_bytes_scanned": cost_est.get("estimated_bytes_scanned", 0),
            "timeout_seconds": constraints.get("timeout_seconds", 30),
            "read_only": constraints.get("read_only", True),
        },
    }
    if team_id:
        plan["team_id"] = team_id
    # Store the visible column list so excavate can post-filter results as defence-in-depth.
    # None means "no column restrictions applied" (all columns public or backend has no schema).
    if allowed_columns is not None:
        plan["allowed_columns"] = allowed_columns
    store_plan(plan_id, plan)

    response_body = {
        "plan_id": plan_id,
        "status": plan_status,
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


def _filter_schema_columns(
    schema: dict, principal_roles: list[str]
) -> tuple[dict, list[str] | None]:
    """Return a filtered copy of schema visible to this principal, plus the allowed column names.

    Visibility levels:
      "public"     — always visible
      "restricted" — requires "pii_access" role
      "phi"        — requires "phi_cleared" role

    Returns:
        (filtered_schema, allowed_columns) where allowed_columns is None if no
        column classification is present (i.e. all columns are implicitly public).
    """
    columns = schema.get("columns", [])
    if not columns:
        return schema, None

    # Check if any column has a non-public visibility tag
    has_restricted = any(col.get("visibility", "public") != "public" for col in columns)
    if not has_restricted:
        # No restricted columns — return schema unchanged, no filtering needed
        return schema, None

    can_access_phi = "phi_cleared" in principal_roles
    can_access_pii = "pii_access" in principal_roles or can_access_phi

    visible_columns = []
    for col in columns:
        vis = col.get("visibility", "public")
        if vis == "phi" and not can_access_phi:
            continue
        if vis == "restricted" and not can_access_pii:
            continue
        visible_columns.append(col)

    allowed_names = [col["name"] for col in visible_columns]
    filtered = {**schema, "columns": visible_columns}
    return filtered, allowed_names


def _backend_to_query_type(backend: str) -> str:
    return {
        "athena": "athena_sql",
        "opensearch": "opensearch_dsl",
        "s3": "s3_select_sql",
        "dynamodb": "dynamodb_partiql",
        "mcp": "mcp_tool",
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

    if query_type == "mcp_tool":
        rules_block = """Rules:
1. The query MUST be a JSON object serialized as a string.
2. Format: {"server": "<server_name>", "tool": "<tool_name>", "arguments": {...}}
3. Server name comes from the source_id: mcp://<server_name>/<resource>.
4. Choose the most appropriate tool from available_tools in the schema.
5. Arguments must conform to the tool's input_schema.
6. Do not fabricate tool names or arguments not present in the schema.

Respond in JSON with exactly this structure:
{{
  "query": "{{\\"server\\": \\"<name>\\", \\"tool\\": \\"<tool>\\", \\"arguments\\": {{...}}}}",
  "output_schema": {{
    "columns": ["col1", "col2"],
    "estimated_rows": 0
  }},
  "reasoning": "<brief explanation>"
}}"""
    else:
        rules_block = """Rules:
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
}}"""

    return f"""You are a query planner for the clAWS excavation system. Your job is to
translate a natural-language objective into a concrete, executable query.

SOURCE: {source_id}
QUERY TYPE: {query_type}

SCHEMA:
{schema_text}

CONSTRAINTS:
{constraints_text}

OBJECTIVE: {objective}

{rules_block}

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
