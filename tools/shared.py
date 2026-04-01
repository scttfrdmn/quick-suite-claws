"""clAWS shared utilities for Lambda handlers."""

import json
import os
import time
import uuid
from datetime import UTC, datetime
from typing import Any

import boto3

# Clients — initialized once per Lambda cold start
_s3 = None
_dynamodb = None
_bedrock = None


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


# --- Configuration ---

RUNS_BUCKET = os.environ.get("CLAWS_RUNS_BUCKET", "claws-runs")
PLANS_TABLE = os.environ.get("CLAWS_PLANS_TABLE", "claws-plans")
SCHEMAS_TABLE = os.environ.get("CLAWS_SCHEMAS_TABLE", "claws-schemas")
GUARDRAIL_ID = os.environ.get("CLAWS_GUARDRAIL_ID", "")
GUARDRAIL_VERSION = os.environ.get("CLAWS_GUARDRAIL_VERSION", "DRAFT")


# --- ID generation ---

def new_plan_id() -> str:
    return f"plan-{uuid.uuid4().hex[:8]}"


def new_run_id() -> str:
    return f"run-{uuid.uuid4().hex[:8]}"


def new_export_id() -> str:
    return f"export-{uuid.uuid4().hex[:8]}"


# --- Audit logging ---

def audit_log(tool: str, principal: str, inputs: dict, outputs: dict,
              cost: float | None = None, guardrail_trace: dict | None = None) -> None:
    """Write a structured audit record. In production this goes to
    CloudWatch Logs / S3 / OpenSearch for compliance."""
    record = {
        "timestamp": datetime.now(UTC).isoformat(),
        "tool": tool,
        "principal": principal,
        "inputs": inputs,
        "outputs": {k: v for k, v in outputs.items() if k != "result_preview"},
        "cost": cost,
        "guardrail_trace": guardrail_trace,
    }
    # For now, structured print — CloudWatch picks this up
    print(json.dumps(record, default=str))


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


def load_result(run_id: str) -> Any:
    """Load excavation results from S3."""
    key = f"{run_id}/result.json"
    obj = s3_client().get_object(Bucket=RUNS_BUCKET, Key=key)
    return json.loads(obj["Body"].read())


# --- Plan storage ---

def store_plan(plan_id: str, plan: dict) -> None:
    """Store an execution plan in DynamoDB."""
    table = dynamodb_resource().Table(PLANS_TABLE)
    table.put_item(Item={
        "plan_id": plan_id,
        "created_at": datetime.now(UTC).isoformat(),
        "ttl": int(time.time()) + 86400,  # 24h TTL
        **plan,
    })


def load_plan(plan_id: str) -> dict | None:
    """Load a plan from DynamoDB."""
    table = dynamodb_resource().Table(PLANS_TABLE)
    resp = table.get_item(Key={"plan_id": plan_id})
    item: dict | None = resp.get("Item")
    return item


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
    """
    if not GUARDRAIL_ID:
        return {"action": "NONE", "assessments": []}

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
    """Scan a JSON-serializable payload in chunks. Returns
    {"status": "clean"|"blocked"|"masked", "payload": ...}."""
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
