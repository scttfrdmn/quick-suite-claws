"""clAWS shared utilities for Lambda handlers."""

import json
import logging
import os
import time
import uuid
from datetime import UTC, datetime
from typing import Any

import boto3

logger = logging.getLogger(__name__)

# Clients — initialized once per Lambda cold start
_s3 = None
_dynamodb = None
_bedrock = None
_cloudwatch = None


def s3_client() -> Any:
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3")
    return _s3


def dynamodb_resource() -> Any:
    global _dynamodb
    if _dynamodb is None:
        _dynamodb = boto3.resource("dynamodb")
    return _dynamodb


def bedrock_runtime() -> Any:
    global _bedrock
    if _bedrock is None:
        _bedrock = boto3.client("bedrock-runtime")
    return _bedrock


def cloudwatch_client() -> Any:
    global _cloudwatch
    if _cloudwatch is None:
        _cloudwatch = boto3.client("cloudwatch")
    return _cloudwatch


def call_router(router_tool: str, prompt: str, max_tokens: int = 2048) -> str | None:
    """Invoke the Quick Suite model router for LLM generation.

    Reads ROUTER_ENDPOINT, ROUTER_TOKEN_URL, ROUTER_SECRET_ARN from the
    environment. The secret must contain JSON with "client_id" and
    "client_secret" for the Cognito M2M client_credentials flow.

    Returns the generated text content, or None if the router is not
    configured or encounters any error (callers fall back to direct Bedrock).
    """
    import urllib.parse
    import urllib.request

    endpoint = os.environ.get("ROUTER_ENDPOINT", "")
    token_url = os.environ.get("ROUTER_TOKEN_URL", "")
    secret_arn = os.environ.get("ROUTER_SECRET_ARN", "")

    if not endpoint or not token_url or not secret_arn:
        return None

    try:
        sm = boto3.client("secretsmanager")
        secret = json.loads(sm.get_secret_value(SecretId=secret_arn)["SecretString"])
        client_id = secret["client_id"]
        client_secret = secret["client_secret"]

        # Obtain OAuth token via client_credentials
        token_data = urllib.parse.urlencode({
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://quicksuite.internal/router",
        }).encode()
        token_req = urllib.request.Request(
            token_url,
            data=token_data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        with urllib.request.urlopen(token_req, timeout=5) as resp:  # noqa: S310
            token = json.loads(resp.read())["access_token"]

        # Call the router
        body = json.dumps({
            "prompt": prompt,
            "max_tokens": max_tokens,
            "temperature": 0.0,
        }).encode()
        router_req = urllib.request.Request(
            f"{endpoint.rstrip('/')}/tools/{router_tool}",
            data=body,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            },
        )
        with urllib.request.urlopen(router_req, timeout=30) as resp:  # noqa: S310
            result = json.loads(resp.read())

        return result.get("content") or None

    except Exception as exc:
        print(json.dumps({"level": "warn", "msg": "call_router failed", "error": str(exc)}))
        return None


# --- Configuration ---

RUNS_BUCKET = os.environ.get("CLAWS_RUNS_BUCKET", "claws-runs")
PLANS_TABLE = os.environ.get("CLAWS_PLANS_TABLE", "claws-plans")
SCHEMAS_TABLE = os.environ.get("CLAWS_SCHEMAS_TABLE", "claws-schemas")
WATCHES_TABLE = os.environ.get("CLAWS_WATCHES_TABLE", "claws-watches")
GUARDRAIL_ID = os.environ.get("CLAWS_GUARDRAIL_ID", "")
GUARDRAIL_VERSION = os.environ.get("CLAWS_GUARDRAIL_VERSION", "DRAFT")
METRICS_NAMESPACE = os.environ.get("CLAWS_METRICS_NAMESPACE", "")

# Warn at module load time if guardrail is unconfigured (#77)
if not GUARDRAIL_ID:
    logger.warning(
        "CLAWS_GUARDRAIL_ID is not configured — guardrail scanning will be bypassed. "
        "All scan_payload() calls will return status='bypassed'."
    )

# --- source_id validation (#78) ---

_SOURCE_ID_PREFIXES = frozenset(
    {"athena:", "dynamodb:", "s3:", "opensearch:", "mcp:", "registry:"}
)
_SOURCE_ID_MAX_LEN = 512


def validate_source_id(source_id: str) -> None:
    """Validate that source_id is well-formed and safe for use in storage keys.

    Raises ValueError with a descriptive message on any violation.
    """
    if not source_id:
        raise ValueError("source_id is required")
    if len(source_id) > _SOURCE_ID_MAX_LEN:
        raise ValueError(
            f"source_id exceeds maximum length of {_SOURCE_ID_MAX_LEN} characters"
        )
    if ".." in source_id:
        raise ValueError("source_id contains invalid path traversal sequence")
    if "\x00" in source_id or any(ord(c) < 32 for c in source_id):
        raise ValueError("source_id contains invalid control characters")
    if not any(source_id.startswith(p) for p in _SOURCE_ID_PREFIXES):
        raise ValueError(
            f"source_id must start with a known prefix: {sorted(_SOURCE_ID_PREFIXES)}"
        )


# --- ID generation ---

def emit_metric(
    metric_name: str,
    value: float,
    unit: str,
    dimensions: list[dict] | None = None,
) -> None:
    """Emit a CloudWatch metric data point.

    Skipped when CLAWS_METRICS_NAMESPACE is unset (dev/test safety).
    Failures are swallowed — metrics must never break a tool call.
    """
    if not METRICS_NAMESPACE:
        return
    try:
        metric: dict = {"MetricName": metric_name, "Value": value, "Unit": unit}
        if dimensions:
            metric["Dimensions"] = dimensions
        cloudwatch_client().put_metric_data(Namespace=METRICS_NAMESPACE, MetricData=[metric])
    except Exception as e:
        print(json.dumps({"level": "warn", "msg": "emit_metric failed", "error": str(e)}))


def new_plan_id() -> str:
    return f"plan-{uuid.uuid4().hex[:8]}"


def new_run_id() -> str:
    return f"run-{uuid.uuid4().hex[:8]}"


def new_export_id() -> str:
    return f"export-{uuid.uuid4().hex[:8]}"


def new_watch_id() -> str:
    return f"watch-{uuid.uuid4().hex[:8]}"


# --- Audit logging ---

def audit_log(
    tool: str,
    principal: str,
    inputs: dict,
    outputs: dict,
    cost: float | None = None,
    guardrail_trace: dict | None = None,
    request_id: str = "",
) -> None:
    """Write a structured audit record. In production this goes to
    CloudWatch Logs / S3 / OpenSearch for compliance."""
    record = {
        "timestamp": datetime.now(UTC).isoformat(),
        "tool": tool,
        "principal": principal,
        "request_id": request_id,
        "inputs": inputs,
        "outputs": {k: v for k, v in outputs.items() if k != "result_preview"},
        "cost": cost,
        "guardrail_trace": guardrail_trace,
    }
    print(json.dumps(record, default=str))

    # Emit CloudWatch metrics — skipped when CLAWS_METRICS_NAMESPACE not set
    _status = outputs.get("status", "complete")
    _dims = [{"Name": "Tool", "Value": tool}]
    emit_metric("Invocations", 1.0, "Count", _dims)
    if _status == "error":
        emit_metric("Errors", 1.0, "Count", _dims)
    elif _status == "blocked":
        emit_metric("GuardrailBlocks", 1.0, "Count", _dims)
    elif _status == "timeout":
        emit_metric("Timeouts", 1.0, "Count", _dims)
    if cost is not None:
        emit_metric("CostDollars", float(cost), "None", _dims)
    if _status == "complete" and "rows_returned" in outputs:
        emit_metric("RowsReturned", float(outputs["rows_returned"]), "Count", _dims)


# --- Result storage ---

def store_result(run_id: str, payload: Any) -> str:
    """Store excavation results in S3 and return the URI."""
    key = f"{run_id}/result.json"
    s3_client().put_object(
        Bucket=RUNS_BUCKET,
        Key=key,
        Body=json.dumps(payload, default=str),
        ContentType="application/json",
    )
    return f"s3://{RUNS_BUCKET}/{key}"


def store_result_metadata(
    run_id: str,
    schema: list[dict],
    row_count: int,
    bytes_scanned: int,
    cost: str,
    source_id: str,
) -> str:
    """Write result_metadata.json alongside result.json. Returns the S3 URI."""
    key = f"{run_id}/result_metadata.json"
    metadata = {
        "run_id": run_id,
        "schema": schema,
        "row_count": row_count,
        "bytes_scanned": bytes_scanned,
        "cost": cost,
        "source_id": source_id,
        "created_at": datetime.now(UTC).isoformat(),
    }
    s3_client().put_object(
        Bucket=RUNS_BUCKET,
        Key=key,
        Body=json.dumps(metadata, default=str),
        ContentType="application/json",
    )
    return f"s3://{RUNS_BUCKET}/{key}"


def load_result(run_id: str) -> Any:
    """Load excavation results from S3."""
    key = f"{run_id}/result.json"
    obj = s3_client().get_object(Bucket=RUNS_BUCKET, Key=key)
    return json.loads(obj["Body"].read())


# --- Plan storage ---

def _clean_item(item: dict) -> dict:
    """Remove None values and empty collections from a DynamoDB item dict.

    DynamoDB NULL type is not used; empty maps/lists cause TypeDeserializer
    failures in some SDK versions.
    """
    return {
        k: v for k, v in item.items()
        if v is not None and not (isinstance(v, (dict, list)) and not v)
    }


def store_plan(plan_id: str, plan: dict) -> None:
    """Store an execution plan in DynamoDB."""
    table = dynamodb_resource().Table(PLANS_TABLE)
    table.put_item(Item=_clean_item({
        "plan_id": plan_id,
        "created_at": datetime.now(UTC).isoformat(),
        "ttl": int(time.time()) + 86400,  # 24h TTL
        **plan,
    }))


def list_plans_by_team(team_id: str) -> list[dict]:
    """Scan the plans table filtered by team_id."""
    table = dynamodb_resource().Table(PLANS_TABLE)
    resp = table.scan(
        FilterExpression="#t = :t",
        ExpressionAttributeNames={"#t": "team_id"},
        ExpressionAttributeValues={":t": team_id},
    )
    return resp.get("Items", [])


def share_plan(plan_id: str, shared_with: list[str]) -> bool:
    """Write shared_with list onto a plan item. Returns False if plan not found."""
    table = dynamodb_resource().Table(PLANS_TABLE)
    resp = table.get_item(Key={"plan_id": plan_id})
    if not resp.get("Item"):
        return False
    table.update_item(
        Key={"plan_id": plan_id},
        UpdateExpression="SET shared_with = :sw",
        ExpressionAttributeValues={":sw": shared_with},
    )
    return True


def load_plan(plan_id: str) -> dict | None:
    """Load a plan from DynamoDB."""
    table = dynamodb_resource().Table(PLANS_TABLE)
    resp = table.get_item(Key={"plan_id": plan_id})
    item: dict | None = resp.get("Item")
    return item


# --- Watch storage ---

def store_watch(watch_id: str, spec: dict) -> None:
    """Store a watch spec in DynamoDB.

    None values and empty collections are omitted — DynamoDB NULL type is not used,
    and empty maps/lists cause TypeDeserializer failures in some SDK versions.
    """
    table = dynamodb_resource().Table(WATCHES_TABLE)
    item = {"watch_id": watch_id}
    for k, v in spec.items():
        if v is None:
            continue
        if isinstance(v, (dict, list)) and not v:
            continue
        item[k] = v
    table.put_item(Item=item)


def load_watch(watch_id: str) -> dict | None:
    """Load a watch spec from DynamoDB."""
    table = dynamodb_resource().Table(WATCHES_TABLE)
    resp = table.get_item(Key={"watch_id": watch_id})
    return resp.get("Item")


def update_watch(watch_id: str, updates: dict) -> None:
    """Apply a dict of attribute updates to an existing watch item.

    None values are skipped — use delete_watch to remove an item entirely.
    """
    updates = {k: v for k, v in updates.items() if v is not None}
    if not updates:
        return
    table = dynamodb_resource().Table(WATCHES_TABLE)
    keys = list(updates.keys())
    vals = list(updates.values())
    set_clauses = ", ".join(f"#k{i} = :v{i}" for i in range(len(keys)))
    names = {f"#k{i}": keys[i] for i in range(len(keys))}
    values = {f":v{i}": vals[i] for i in range(len(vals))}
    table.update_item(
        Key={"watch_id": watch_id},
        UpdateExpression=f"SET {set_clauses}",
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )


def delete_watch(watch_id: str) -> None:
    """Delete a watch item from DynamoDB."""
    table = dynamodb_resource().Table(WATCHES_TABLE)
    table.delete_item(Key={"watch_id": watch_id})


def list_watches(
    status_filter: str | None = None,
    team_id_filter: str | None = None,
) -> list[dict]:
    """Scan the watches table; optionally filter by status and/or team_id."""
    table = dynamodb_resource().Table(WATCHES_TABLE)

    filter_parts = []
    names: dict = {}
    values: dict = {}

    if status_filter:
        filter_parts.append("#s = :s")
        names["#s"] = "status"
        values[":s"] = status_filter

    if team_id_filter:
        filter_parts.append("#tid = :tid")
        names["#tid"] = "team_id"
        values[":tid"] = team_id_filter

    if filter_parts:
        filter_expr = " AND ".join(filter_parts)
        resp = table.scan(
            FilterExpression=filter_expr,
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
        )
    else:
        resp = table.scan()

    items: list[dict] = resp.get("Items", [])
    return items


# --- Schema cache ---

def get_cached_schema(source_id: str) -> dict | None:
    """Retrieve a cached schema from DynamoDB (populated by probe)."""
    table = dynamodb_resource().Table(SCHEMAS_TABLE)
    resp = table.get_item(Key={"source_id": source_id})
    schema: dict | None = resp.get("Item", {}).get("schema")
    return schema


def cache_schema(source_id: str, schema: dict) -> None:
    """Cache a source schema in DynamoDB."""
    table = dynamodb_resource().Table(SCHEMAS_TABLE)
    table.put_item(Item={
        "source_id": source_id,
        "cached_at": datetime.now(UTC).isoformat(),
        "ttl": int(time.time()) + 3600,  # 1h TTL
        "schema": schema,
    })


# --- Guardrail scanning ---

def apply_guardrail(content: str, source: str = "OUTPUT") -> dict:
    """Scan content with Bedrock Guardrails ApplyGuardrail API.

    Returns:
        {"action": "NONE" | "GUARDRAIL_INTERVENED", "assessments": [...]}
        When GUARDRAIL_ID is unconfigured, returns with "bypassed": True (#77).
    """
    if not GUARDRAIL_ID:
        logger.error(
            "apply_guardrail called but CLAWS_GUARDRAIL_ID is not configured — "
            "guardrail check bypassed"
        )
        return {"action": "NONE", "assessments": [], "bypassed": True}

    response = bedrock_runtime().apply_guardrail(
        guardrailIdentifier=GUARDRAIL_ID,
        guardrailVersion=GUARDRAIL_VERSION,
        source=source,
        content=[{"text": {"text": content}}],
    )
    return {
        "action": response.get("action", "NONE"),
        "assessments": response.get("assessments", []),
    }


def scan_payload(payload: Any, max_chunk_chars: int = 25000) -> dict:
    """Scan a JSON-serializable payload in chunks.

    Returns:
        {"status": "clean"|"blocked"|"bypassed", "payload": ...}

    When GUARDRAIL_ID is unconfigured, returns status="bypassed" instead of
    "clean" so callers can distinguish a real clean scan from an unconfigured
    one (#77). All existing callers check status=="blocked" only, so this is
    backward-compatible.
    """
    if not GUARDRAIL_ID:
        logger.error(
            "scan_payload called but CLAWS_GUARDRAIL_ID is not configured — "
            "guardrail scanning bypassed"
        )
        return {"status": "bypassed", "payload": payload}

    text = json.dumps(payload, default=str)

    if len(text) <= max_chunk_chars:
        result = apply_guardrail(text)
        if result["action"] == "GUARDRAIL_INTERVENED":
            return {"status": "blocked", "assessments": result["assessments"]}
        return {"status": "clean", "payload": payload}

    # Chunk and scan
    for i in range(0, len(text), max_chunk_chars):
        chunk = text[i:i + max_chunk_chars]
        result = apply_guardrail(chunk)
        if result["action"] == "GUARDRAIL_INTERVENED":
            return {"status": "blocked", "assessments": result["assessments"]}

    return {"status": "clean", "payload": payload}


# --- Drift detection ---

def diff_results(uri_a: str, uri_b: str, key_column: str) -> dict:
    """Compare two S3 NDJSON/JSON result sets by key_column.

    Loads both URIs, computes added/removed/changed row sets, and returns a
    summary dict with counts and sample rows (up to 5 per category).

    Returns:
        {
            "added":   [...up to 5 sample rows present in B but not A...],
            "removed": [...up to 5 sample rows present in A but not B...],
            "changed": [...up to 5 sample rows present in both but with different values...],
            "added_count": N,
            "removed_count": N,
            "changed_count": N,
            "unchanged_count": N,
        }
    """

    def _load(uri: str) -> list[dict]:
        parts = uri.replace("s3://", "").split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""
        obj = s3_client().get_object(Bucket=bucket, Key=key)
        raw = obj["Body"].read().decode("utf-8").strip()
        try:
            data = json.loads(raw)
            return data if isinstance(data, list) else [data]
        except json.JSONDecodeError:
            return [json.loads(line) for line in raw.splitlines() if line.strip()]

    rows_a = _load(uri_a)
    rows_b = _load(uri_b)

    # Index by key_column
    index_a: dict = {str(row.get(key_column, i)): row for i, row in enumerate(rows_a)}
    index_b: dict = {str(row.get(key_column, i)): row for i, row in enumerate(rows_b)}

    keys_a = set(index_a)
    keys_b = set(index_b)

    removed_keys = keys_a - keys_b
    added_keys = keys_b - keys_a
    common_keys = keys_a & keys_b

    changed: list[dict] = []
    unchanged_count = 0
    for k in common_keys:
        if json.dumps(index_a[k], sort_keys=True, default=str) != json.dumps(index_b[k], sort_keys=True, default=str):
            changed.append(index_b[k])
        else:
            unchanged_count += 1

    added_rows = [index_b[k] for k in sorted(added_keys)]
    removed_rows = [index_a[k] for k in sorted(removed_keys)]

    return {
        "added": added_rows[:5],
        "removed": removed_rows[:5],
        "changed": changed[:5],
        "added_count": len(added_rows),
        "removed_count": len(removed_rows),
        "changed_count": len(changed),
        "unchanged_count": unchanged_count,
    }


# --- Lambda response helpers ---

def success(body: dict, status_code: int = 200) -> dict:
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body, default=str),
    }


def error(message: Any, status_code: int = 400) -> dict:
    from tools.errors import ClawsError
    if isinstance(message, ClawsError):
        return {
            "statusCode": message.status_code,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": message.message}),
        }
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"error": message}),
    }
