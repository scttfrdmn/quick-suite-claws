"""S3 Select executor for clAWS excavate tool.

Executes SQL expressions against S3 objects via the S3 Select API.
Supports CSV, JSON Lines, and Parquet input formats.
No new runtime dependencies — pure boto3.
"""

import json

from tools.shared import s3_client

# S3 Select pricing
_PRICE_PER_BYTE_SCANNED = 0.002 / (1024 ** 3)   # $0.002 / GB scanned
_PRICE_PER_BYTE_RETURNED = 0.0007 / (1024 ** 3)  # $0.0007 / GB returned


def _parse_source_id(source_id: str) -> tuple[str, str]:
    """Parse 's3://bucket/key' or 's3:bucket/key' → (bucket, key)."""
    uri = source_id
    if uri.startswith("s3://"):
        uri = uri[5:]
    elif uri.startswith("s3:"):
        uri = uri[3:]
    bucket, _, key = uri.partition("/")
    return bucket, key


def _input_serialization(key: str, constraints: dict) -> dict:
    """Detect input format from key extension or explicit constraint."""
    fmt = constraints.get("input_format", "").lower()
    if not fmt:
        if key.endswith(".parquet"):
            fmt = "parquet"
        elif key.endswith(".json") or key.endswith(".jsonl"):
            fmt = "json"
        else:
            fmt = "csv"  # default

    if fmt == "parquet":
        return {"Parquet": {}}
    elif fmt == "json":
        return {"JSON": {"Type": "LINES"}}
    else:
        return {"CSV": {"FileHeaderInfo": "USE"}}


def execute_s3_select(
    source_id: str,
    query: str,
    constraints: dict,
    run_id: str,
) -> dict:
    """Execute a SQL expression against an S3 object via S3 Select.

    Args:
        source_id: Source identifier, e.g. "s3://bucket/path/to/file.csv"
        query: SQL expression, e.g. "SELECT * FROM S3Object WHERE age > 30"
        constraints: Dict with optional keys:
            - max_bytes_scanned: int — ScanRange end byte
            - input_format: str — "csv", "json", or "parquet" (auto-detected otherwise)
        run_id: clAWS run identifier (unused here, reserved for future use)

    Returns:
        {
            "status": "complete" | "error",
            "rows": list[dict],          # on success
            "bytes_scanned": int,
            "cost": str,                 # e.g. "$0.0001"
            "error": str,                # on error only
        }
    """
    try:
        bucket, key = _parse_source_id(source_id)
    except Exception as e:
        return {"status": "error", "error": f"Could not parse source_id '{source_id}': {e}"}

    if not bucket or not key:
        return {
            "status": "error",
            "error": f"Invalid S3 source_id (missing bucket or key): {source_id}",
        }

    input_ser = _input_serialization(key, constraints)

    select_kwargs: dict = {
        "Bucket": bucket,
        "Key": key,
        "ExpressionType": "SQL",
        "Expression": query,
        "InputSerialization": input_ser,
        "OutputSerialization": {"JSON": {"RecordDelimiter": "\n"}},
    }

    # Apply byte-scan limit via ScanRange if provided
    max_bytes = constraints.get("max_bytes_scanned")
    if max_bytes:
        select_kwargs["ScanRange"] = {"Start": 0, "End": int(max_bytes)}

    try:
        response = s3_client().select_object_content(**select_kwargs)
    except s3_client().exceptions.NoSuchKey:
        return {"status": "error", "error": f"Object not found: s3://{bucket}/{key}"}
    except Exception as e:
        error_code = (getattr(e, "response", {}) or {}).get("Error", {}).get("Code", "")
        if error_code == "NoSuchKey":
            return {"status": "error", "error": f"Object not found: s3://{bucket}/{key}"}
        return {"status": "error", "error": f"S3 Select failed: {e}"}

    # Stream and collect records
    rows: list[dict] = []
    bytes_scanned = 0
    bytes_returned = 0

    try:
        for event in response["Payload"]:
            if "Records" in event:
                raw = event["Records"]["Payload"].decode("utf-8")
                for line in raw.split("\n"):
                    line = line.strip()
                    if line:
                        import contextlib
                        with contextlib.suppress(json.JSONDecodeError):
                            rows.append(json.loads(line))
            elif "Stats" in event:
                stats = event["Stats"]["Details"]
                bytes_scanned = stats.get("BytesScanned", 0)
                bytes_returned = stats.get("BytesReturned", 0)
    except Exception as e:
        return {"status": "error", "error": f"Failed to stream S3 Select results: {e}"}

    cost = bytes_scanned * _PRICE_PER_BYTE_SCANNED + bytes_returned * _PRICE_PER_BYTE_RETURNED

    return {
        "status": "complete",
        "rows": rows,
        "bytes_scanned": bytes_scanned,
        "cost": f"${cost:.4f}",
    }
