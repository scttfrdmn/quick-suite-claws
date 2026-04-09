"""clAWS remember tool — write structured findings to institutional memory store.

Appends a JSON record to s3://{CLAWS_MEMORY_BUCKET}/{account_id}/{user_arn_hash}/findings.jsonl
using an ETag conditional write to prevent concurrent-write data loss (up to 3 retries).

First write for a user triggers the data-side register-memory-source Lambda to register
the NDJSON file as a QuickSight SPICE dataset so findings are queryable in Quick Suite.
"""
from __future__ import annotations

import json
import logging
import os
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import boto3
from botocore.exceptions import ClientError

from tools.shared import audit_log, error, success

logger = logging.getLogger(__name__)

CLAWS_MEMORY_BUCKET = os.environ.get("CLAWS_MEMORY_BUCKET", "")
CLAWS_MEMORY_REGISTRY_TABLE = os.environ.get("CLAWS_MEMORY_REGISTRY_TABLE", "claws-memory-registry")
MEMORY_REGISTRAR_ARN = os.environ.get("MEMORY_REGISTRAR_ARN", "")

_s3 = None
_dynamodb = None
_lambda_client = None


def _s3_client():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3")
    return _s3


def _ddb():
    global _dynamodb
    if _dynamodb is None:
        _dynamodb = boto3.resource("dynamodb")
    return _dynamodb


def _lambda():
    global _lambda_client
    if _lambda_client is None:
        _lambda_client = boto3.client("lambda")
    return _lambda_client


def handler(event: dict, context: Any) -> dict:
    """Lambda handler for claws.remember."""
    body = json.loads(event.get("body", "{}")) if isinstance(event.get("body"), str) else event
    principal = event.get("requestContext", {}).get("authorizer", {}).get("principalId", "unknown")
    request_id = event.get("requestContext", {}).get("requestId", "")

    subject = str(body.get("subject", "")).strip()
    if not subject:
        return error("subject is required")

    fact = str(body.get("fact", "")).strip()
    confidence = float(body.get("confidence", 0.8))
    tags: list[str] = body.get("tags") or []
    if isinstance(tags, str):
        tags = [tags]
    source_plan_id = str(body.get("source_plan_id", "")).strip()
    severity = str(body.get("severity", "info")).strip()
    expires_days = int(body.get("expires_days", 365))
    user_arn_hash = str(body.get("user_arn_hash", "")).strip()
    account_id = str(body.get("account_id", "")).strip()

    if not user_arn_hash or not account_id:
        return error("user_arn_hash and account_id are required")
    if not CLAWS_MEMORY_BUCKET:
        return error("CLAWS_MEMORY_BUCKET is not configured", status_code=503)

    memory_id = str(uuid.uuid4())[:8]
    now = datetime.now(UTC)
    record = {
        "memory_id": memory_id,
        "subject": subject,
        "fact": fact,
        "confidence": confidence,
        "tags": tags,
        "source_plan_id": source_plan_id,
        "severity": severity,
        "recorded_at": now.isoformat(),
        "expires_at": (now + timedelta(days=expires_days)).isoformat(),
    }

    s3_key = f"{account_id}/{user_arn_hash}/findings.jsonl"

    # ETag conditional write — up to 3 retries on concurrent-write conflict
    registered_dataset_id: str | None = None
    for attempt in range(3):
        try:
            # Try to get existing file + ETag
            try:
                existing = _s3_client().get_object(Bucket=CLAWS_MEMORY_BUCKET, Key=s3_key)
                existing_body = existing["Body"].read()
                etag = existing["ETag"]
                new_body = existing_body + b"\n" + json.dumps(record).encode()
                put_kwargs: dict[str, Any] = {"IfMatch": etag}
            except ClientError as e:
                if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                    # First write for this user
                    existing_body = b""
                    new_body = json.dumps(record).encode()
                    put_kwargs = {}
                else:
                    raise

            _s3_client().put_object(
                Bucket=CLAWS_MEMORY_BUCKET,
                Key=s3_key,
                Body=new_body,
                ContentType="application/x-ndjson",
                **put_kwargs,
            )
            break  # success

        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code == "PreconditionFailed" and attempt < 2:
                time.sleep(0.05 * (attempt + 1))
                continue
            logger.error(f"remember: S3 write failed after {attempt+1} attempts: {e}")
            return error(f"Failed to write memory record: {e}", status_code=500)

    # Check registry — first write triggers QuickSight registration
    s3_uri = f"s3://{CLAWS_MEMORY_BUCKET}/{s3_key}"
    if MEMORY_REGISTRAR_ARN:
        try:
            table = _ddb().Table(CLAWS_MEMORY_REGISTRY_TABLE)
            resp = table.get_item(
                Key={"user_arn_hash": user_arn_hash, "dataset_type": "findings"}
            )
            if "Item" in resp:
                registered_dataset_id = resp["Item"].get("dataset_id")
            else:
                # First write — invoke registrar
                payload = {
                    "user_arn_hash": user_arn_hash,
                    "memory_s3_uri": s3_uri,
                    "dataset_label": "claws-memory",
                    "aws_account_id": account_id,
                }
                invoke_resp = _lambda().invoke(
                    FunctionName=MEMORY_REGISTRAR_ARN,
                    InvocationType="RequestResponse",
                    Payload=json.dumps(payload).encode(),
                )
                reg_result = json.loads(invoke_resp["Payload"].read())
                registered_dataset_id = reg_result.get("dataset_id")
        except Exception as exc:
            logger.warning(f"remember: registry/registrar call failed (non-fatal): {exc}")

    audit_log(
        "remember",
        principal,
        {"subject": subject, "severity": severity},
        {"memory_id": memory_id, "s3_uri": s3_uri},
        request_id=request_id,
    )

    return success({
        "memory_id": memory_id,
        "s3_uri": s3_uri,
        "registered_dataset_id": registered_dataset_id,
    })
