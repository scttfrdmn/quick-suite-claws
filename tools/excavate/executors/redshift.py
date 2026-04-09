"""Redshift executor for clAWS excavate tool.

Executes read-only SQL queries against Redshift Serverless via the
Redshift Data API (async execute-and-poll pattern).
"""

import logging
import os
import re
import time
from typing import Any

import boto3

logger = logging.getLogger(__name__)

_redshift_client = None

WORKGROUP = os.environ.get("CLAWS_REDSHIFT_WORKGROUP", "")
DATABASE = os.environ.get("CLAWS_REDSHIFT_DATABASE", "")
POLL_INTERVAL = 2  # seconds
MAX_POLL_TIME = 300  # seconds

# Redshift pricing: same as Athena ($5/TB)
PRICE_PER_BYTE = 5.0 / (1024**4)

_MUTATION_PATTERN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|GRANT|REVOKE)\b",
    re.IGNORECASE,
)


def _redshift() -> Any:
    global _redshift_client
    if _redshift_client is None:
        _redshift_client = boto3.client("redshift-data")
    return _redshift_client


def _check_mutation(query: str) -> str | None:
    match = _MUTATION_PATTERN.search(query)
    return match.group(0) if match else None


def execute_redshift(
    source_id: str,
    query: str,
    constraints: dict,
    run_id: str,
) -> dict:
    """Execute a read-only SQL query via Redshift Data API."""
    mutation = _check_mutation(query)
    if mutation:
        return {
            "status": "error",
            "error": f"Mutation detected: {mutation}. Only SELECT queries are allowed.",
            "rows": [],
            "row_count": 0,
            "bytes_scanned": 0,
            "cost": "$0.0000",
        }

    if not WORKGROUP or not DATABASE:
        return {
            "status": "error",
            "error": "Redshift workgroup or database not configured",
            "rows": [],
            "row_count": 0,
            "bytes_scanned": 0,
            "cost": "$0.0000",
        }

    client = _redshift()

    try:
        # Execute statement
        exec_resp = client.execute_statement(
            WorkgroupName=WORKGROUP,
            Database=DATABASE,
            Sql=query,
        )
        statement_id = exec_resp["Id"]

        # Poll for completion
        elapsed = 0
        while elapsed < MAX_POLL_TIME:
            desc = client.describe_statement(Id=statement_id)
            status = desc["Status"]
            if status == "FINISHED":
                break
            if status in ("FAILED", "ABORTED"):
                return {
                    "status": "error",
                    "error": "Query execution failed",
                    "rows": [],
                    "row_count": 0,
                    "bytes_scanned": 0,
                    "cost": "$0.0000",
                }
            time.sleep(POLL_INTERVAL)
            elapsed += POLL_INTERVAL
        else:
            return {
                "status": "timeout",
                "error": f"Query timed out after {MAX_POLL_TIME}s",
                "rows": [],
                "row_count": 0,
                "bytes_scanned": 0,
                "cost": "$0.0000",
            }

        # Fetch results
        result = client.get_statement_result(Id=statement_id)
        columns = [col["name"] for col in result.get("ColumnMetadata", [])]
        rows = []
        for record in result.get("Records", []):
            row = {}
            for i, field in enumerate(record):
                col_name = columns[i] if i < len(columns) else f"col_{i}"
                # Redshift Data API returns typed fields
                if "stringValue" in field:
                    row[col_name] = field["stringValue"]
                elif "longValue" in field:
                    row[col_name] = field["longValue"]
                elif "doubleValue" in field:
                    row[col_name] = field["doubleValue"]
                elif "booleanValue" in field:
                    row[col_name] = field["booleanValue"]
                elif "isNull" in field and field["isNull"]:
                    row[col_name] = None
                else:
                    row[col_name] = str(field)
            rows.append(row)

        result_size = desc.get("ResultSize", 0)

        return {
            "status": "complete",
            "rows": rows,
            "schema": [{"name": c, "type": "string"} for c in columns],
            "row_count": len(rows),
            "bytes_scanned": result_size,
            "cost": f"${result_size * PRICE_PER_BYTE:.4f}",
        }
    except Exception as exc:
        logger.error("Redshift execution failed: %s", exc)
        return {
            "status": "error",
            "error": "Query execution failed",
            "rows": [],
            "row_count": 0,
            "bytes_scanned": 0,
            "cost": "$0.0000",
        }
