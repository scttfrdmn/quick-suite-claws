"""Athena executor for clAWS excavate tool.

Runs SQL queries against Athena using a read-only workgroup with
byte-scan limits enforced at the workgroup level.
"""

import os
import time

import boto3

ATHENA_CLIENT = None
S3_CLIENT = None

WORKGROUP = os.environ.get("CLAWS_ATHENA_WORKGROUP", "claws-readonly")
OUTPUT_LOCATION = os.environ.get("CLAWS_ATHENA_OUTPUT", "s3://claws-athena-results/")

# Athena pricing
PRICE_PER_BYTE = 5.0 / (1024 ** 4)  # $5 per TB


def _athena():
    global ATHENA_CLIENT
    if ATHENA_CLIENT is None:
        ATHENA_CLIENT = boto3.client("athena")
    return ATHENA_CLIENT


def _s3():
    global S3_CLIENT
    if S3_CLIENT is None:
        S3_CLIENT = boto3.client("s3")
    return S3_CLIENT


def execute_athena(
    source_id: str,
    query: str,
    constraints: dict,
    run_id: str,
) -> dict:
    """Execute a SQL query on Athena.

    The workgroup enforces:
    - Read-only access (IAM role has no write permissions)
    - Byte scan limits (workgroup configuration)
    - Query timeout (workgroup configuration)

    Args:
        source_id: Qualified source ID (e.g., "athena:db.table")
        query: The SQL query to execute
        constraints: Excavation constraints (max_bytes_scanned, timeout_seconds, read_only)
        run_id: The clAWS run ID for result storage

    Returns:
        {
            "status": "complete" | "error" | "timeout",
            "rows": [...],
            "bytes_scanned": int,
            "cost": str,
        }
    """
    timeout = constraints.get("timeout_seconds", 30)

    try:
        # Start query execution in the read-only workgroup
        response = _athena().start_query_execution(
            QueryString=query,
            WorkGroup=WORKGROUP,
            ResultConfiguration={
                "OutputLocation": f"{OUTPUT_LOCATION}{run_id}/",
            },
        )
        execution_id = response["QueryExecutionId"]

    except Exception as e:
        return {"status": "error", "error": f"Failed to start query: {e}"}

    # Poll for completion
    start_time = time.time()
    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout:
            # Attempt to cancel
            try:
                _athena().stop_query_execution(QueryExecutionId=execution_id)
            except Exception:
                pass
            return {"status": "timeout", "error": f"Query timed out after {timeout}s"}

        try:
            status_resp = _athena().get_query_execution(QueryExecutionId=execution_id)
            state = status_resp["QueryExecution"]["Status"]["State"]

            if state == "SUCCEEDED":
                break
            elif state in ("FAILED", "CANCELLED"):
                reason = status_resp["QueryExecution"]["Status"].get(
                    "StateChangeReason", "Unknown"
                )
                return {"status": "error", "error": f"Query {state}: {reason}"}

        except Exception as e:
            return {"status": "error", "error": f"Status check failed: {e}"}

        time.sleep(1)

    # Get statistics
    stats = status_resp["QueryExecution"].get("Statistics", {})
    bytes_scanned = stats.get("DataScannedInBytes", 0)
    cost = bytes_scanned * PRICE_PER_BYTE

    # Fetch results
    rows = []
    try:
        paginator = _athena().get_paginator("get_query_results")
        page_iter = paginator.paginate(QueryExecutionId=execution_id)

        columns = None
        for page in page_iter:
            result_set = page["ResultSet"]

            if columns is None:
                columns = [
                    col["Name"]
                    for col in result_set["ResultSetMetadata"]["ColumnInfo"]
                ]

            for row in result_set["Rows"]:
                values = [datum.get("VarCharValue", "") for datum in row["Data"]]

                # Skip header row
                if values == columns:
                    continue

                rows.append(dict(zip(columns, values)))

    except Exception as e:
        return {
            "status": "error",
            "error": f"Failed to fetch results: {e}",
            "bytes_scanned": bytes_scanned,
            "cost": f"${cost:.4f}",
        }

    return {
        "status": "complete",
        "rows": rows,
        "bytes_scanned": bytes_scanned,
        "cost": f"${cost:.4f}",
    }
