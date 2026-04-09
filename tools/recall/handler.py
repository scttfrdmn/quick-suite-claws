"""clAWS recall tool — query institutional memory with structural filters.

Reads the user's NDJSON findings file from S3 and filters records by:
  - expires_at > now (exclude expired)
  - recorded_at >= now - since_days
  - severity in filter list
  - tags: any-match
  - query: substring match on subject + fact

Returns {records, total, filtered}.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime, timedelta
from typing import Any

import boto3
from botocore.exceptions import ClientError

from tools.shared import error, success

logger = logging.getLogger(__name__)

CLAWS_MEMORY_BUCKET = os.environ.get("CLAWS_MEMORY_BUCKET", "")

_s3 = None


def _s3_client():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3")
    return _s3


def handler(event: dict, context: Any) -> dict:
    """Lambda handler for claws.recall."""
    body = json.loads(event.get("body", "{}")) if isinstance(event.get("body"), str) else event

    query = str(body.get("query", "")).strip().lower()
    filter_tags: list[str] = body.get("tags") or []
    if isinstance(filter_tags, str):
        filter_tags = [filter_tags]
    since_days = int(body.get("since_days", 90))
    filter_severity: list[str] = body.get("severity") or []
    if isinstance(filter_severity, str):
        filter_severity = [filter_severity]
    limit = int(body.get("limit", 50))
    user_arn_hash = str(body.get("user_arn_hash", "")).strip()
    account_id = str(body.get("account_id", "")).strip()

    if not user_arn_hash or not account_id:
        return error("user_arn_hash and account_id are required")
    if not CLAWS_MEMORY_BUCKET:
        return error("CLAWS_MEMORY_BUCKET is not configured", status_code=503)

    s3_key = f"{account_id}/{user_arn_hash}/findings.jsonl"

    # Load NDJSON
    try:
        resp = _s3_client().get_object(Bucket=CLAWS_MEMORY_BUCKET, Key=s3_key)
        raw = resp["Body"].read().decode()
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return success({"records": [], "total": 0, "filtered": 0})
        logger.error(f"recall: S3 read failed: {e}")
        return error(f"Failed to read memory: {e}", status_code=500)

    # Parse records
    now = datetime.now(UTC)
    cutoff = now - timedelta(days=since_days)
    all_records: list[dict] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
            all_records.append(rec)
        except json.JSONDecodeError:
            logger.warning("recall: skipping malformed NDJSON line")
    total = len(all_records)

    # Filter pipeline
    filtered: list[dict] = []
    for rec in all_records:
        # Expiry filter
        try:
            expires_at = datetime.fromisoformat(rec.get("expires_at", "9999-12-31T00:00:00+00:00"))
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=UTC)
            if expires_at <= now:
                continue
        except Exception:
            pass

        # Since-days filter
        try:
            recorded_at = datetime.fromisoformat(
                rec.get("recorded_at", "1970-01-01T00:00:00+00:00")
            )
            if recorded_at.tzinfo is None:
                recorded_at = recorded_at.replace(tzinfo=UTC)
            if recorded_at < cutoff:
                continue
        except Exception:
            pass

        # Severity filter
        if filter_severity and rec.get("severity") not in filter_severity:
            continue

        # Tag any-match
        if filter_tags:
            rec_tags = rec.get("tags") or []
            if not any(t in rec_tags for t in filter_tags):
                continue

        # Query substring match
        if query:
            searchable = (rec.get("subject", "") + " " + rec.get("fact", "")).lower()
            if query not in searchable:
                continue

        filtered.append(rec)

    return success({
        "records": filtered[:limit],
        "total": total,
        "filtered": len(filtered[:limit]),
    })
