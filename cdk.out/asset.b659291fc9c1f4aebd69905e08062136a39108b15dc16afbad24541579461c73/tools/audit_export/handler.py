"""clAWS audit_export — internal Lambda for compliance audit record export.

NOT an AgentCore tool. Invoked by compliance officers or automated
compliance pipelines via direct Lambda invocation.

Scans CloudWatch Logs for clAWS audit records written by audit_log()
in shared.py. Each record is a JSON line printed to stdout (captured by
CloudWatch Logs). The Lambda finds log events in the given date range,
parses them, hashes the inputs and outputs (no PII in the export),
and writes NDJSON to the specified S3 URI.

Output record schema per line:
  {
    "principal":      string,
    "tool":           string,
    "inputs_hash":    string (SHA-256 hex of inputs JSON),
    "outputs_hash":   string (SHA-256 hex of outputs JSON),
    "cost_usd":       number | null,
    "guardrail_trace": bool (true if guardrail_trace was non-null/non-empty),
    "timestamp":      ISO-8601 string
  }
"""

import hashlib
import json
import os
from datetime import UTC, datetime
from typing import Any

import boto3

from tools.errors import ValidationError
from tools.shared import error, s3_client, success

# CloudWatch Logs log group where audit records are written.
# Lambda stdout is captured at /aws/lambda/<function-name>; the audit_log()
# helper writes to stdout, so for production the log group is derived from
# the invoking Lambda's name. Override with CLAWS_AUDIT_LOG_GROUP env var.
AUDIT_LOG_GROUP = os.environ.get(
    "CLAWS_AUDIT_LOG_GROUP",
    "/aws/lambda/claws-audit",
)


def handler(event: dict, context: Any) -> dict:
    """Lambda handler for clAWS internal audit_export action.

    Input keys:
      - start_date    (required) — YYYY-MM-DD inclusive lower bound
      - end_date      (required) — YYYY-MM-DD inclusive upper bound
      - output_s3_uri (required) — s3://bucket/prefix/file.ndjson
    """
    body = json.loads(event.get("body", "{}")) if isinstance(event.get("body"), str) else event
    start_date = body.get("start_date", "")
    end_date = body.get("end_date", "")
    output_s3_uri = body.get("output_s3_uri", "")

    if not start_date:
        return error(ValidationError("start_date is required (YYYY-MM-DD)"))
    if not end_date:
        return error(ValidationError("end_date is required (YYYY-MM-DD)"))
    if not output_s3_uri:
        return error(ValidationError("output_s3_uri is required"))

    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=UTC)
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(
            hour=23, minute=59, second=59, tzinfo=UTC
        )
    except ValueError as exc:
        return error(ValidationError(f"Invalid date format: {exc}"))

    if start_dt > end_dt:
        return error(ValidationError("start_date must be on or before end_date"))

    # Fetch audit records from CloudWatch Logs
    records = _fetch_audit_records(start_dt, end_dt)

    # Serialise to NDJSON
    ndjson_lines = []
    for record in records:
        ndjson_lines.append(json.dumps(_sanitise_record(record), default=str))

    ndjson_body = "\n".join(ndjson_lines)
    if ndjson_lines:
        ndjson_body += "\n"

    # Write to S3
    _write_to_s3(output_s3_uri, ndjson_body)

    return success({
        "status": "complete",
        "record_count": len(records),
        "output_s3_uri": output_s3_uri,
        "start_date": start_date,
        "end_date": end_date,
    })


def _fetch_audit_records(start_dt: datetime, end_dt: datetime) -> list[dict]:
    """Scan CloudWatch Logs log streams within the date range and return
    parsed audit records (lines where 'tool' and 'principal' are present)."""
    logs = boto3.client("logs")
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    records: list[dict] = []

    try:
        # List log streams — paginate if necessary
        paginator = logs.get_paginator("describe_log_streams")
        stream_pages = paginator.paginate(
            logGroupName=AUDIT_LOG_GROUP,
            orderBy="LastEventTime",
            descending=True,
        )
        stream_names: list[str] = []
        for page in stream_pages:
            for stream in page.get("logStreams", []):
                last_event = stream.get("lastEventTimestamp", 0)
                first_event = stream.get("firstEventTimestamp", 0)
                # Only include streams that overlap the requested date range
                if last_event >= start_ms and first_event <= end_ms:
                    stream_names.append(stream["logStreamName"])
    except Exception as exc:
        # Log group may not exist yet — return empty list
        print(json.dumps({
            "level": "warn",
            "msg": "audit_export: could not list log streams",
            "log_group": AUDIT_LOG_GROUP,
            "error": str(exc),
        }))
        return []

    for stream_name in stream_names:
        try:
            paginator = logs.get_paginator("filter_log_events")
            event_pages = paginator.paginate(
                logGroupName=AUDIT_LOG_GROUP,
                logStreamNames=[stream_name],
                startTime=start_ms,
                endTime=end_ms,
            )
            for page in event_pages:
                for event in page.get("events", []):
                    msg = event.get("message", "").strip()
                    try:
                        parsed = json.loads(msg)
                        # Only include records that look like audit_log() output
                        if "tool" in parsed and "principal" in parsed and "timestamp" in parsed:
                            records.append(parsed)
                    except (json.JSONDecodeError, TypeError):
                        pass
        except Exception as exc:
            print(json.dumps({
                "level": "warn",
                "msg": "audit_export: error reading log stream",
                "stream": stream_name,
                "error": str(exc),
            }))

    return records


def _sha256_of(obj: Any) -> str:
    """Return the SHA-256 hex digest of the canonical JSON representation."""
    canonical = json.dumps(obj, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _sanitise_record(record: dict) -> dict:
    """Convert a raw audit_log record to a compliance export record.

    Inputs and outputs are replaced with their SHA-256 hashes so no PII
    or query content appears in the export file.
    """
    inputs = record.get("inputs", {})
    outputs = record.get("outputs", {})
    cost = record.get("cost")
    guardrail_trace = record.get("guardrail_trace")

    return {
        "principal": record.get("principal", ""),
        "tool": record.get("tool", ""),
        "inputs_hash": _sha256_of(inputs),
        "outputs_hash": _sha256_of(outputs),
        "cost_usd": float(cost) if cost is not None else None,
        "guardrail_trace": bool(guardrail_trace),
        "timestamp": record.get("timestamp", ""),
    }


def _write_to_s3(s3_uri: str, body: str) -> None:
    """Write a string body to an S3 URI (s3://bucket/key)."""
    uri = s3_uri.removeprefix("s3://")
    parts = uri.split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else "audit_export.ndjson"
    s3_client().put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )
