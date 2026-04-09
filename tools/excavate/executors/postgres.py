"""PostgreSQL executor for clAWS excavate tool.

Executes read-only SQL queries against PostgreSQL/Aurora PostgreSQL.
Connection params from Secrets Manager. New connection per invocation.
"""

import json
import logging
import os
import re
from typing import Any

import boto3

logger = logging.getLogger(__name__)

_secrets_client = None
_MUTATION_PATTERN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|GRANT|REVOKE)\b",
    re.IGNORECASE,
)

POSTGRES_SECRET_ARN = os.environ.get("CLAWS_POSTGRES_SECRET_ARN", "")


def _secrets() -> Any:
    global _secrets_client
    if _secrets_client is None:
        _secrets_client = boto3.client("secretsmanager")
    return _secrets_client


def _check_mutation(query: str) -> str | None:
    """Return the offending keyword if mutation detected, else None."""
    match = _MUTATION_PATTERN.search(query)
    return match.group(0) if match else None


def _get_connection_params() -> dict:
    """Fetch PostgreSQL connection params from Secrets Manager."""
    if not POSTGRES_SECRET_ARN:
        raise RuntimeError("CLAWS_POSTGRES_SECRET_ARN not configured")
    resp = _secrets().get_secret_value(SecretId=POSTGRES_SECRET_ARN)
    return json.loads(resp["SecretString"])


def execute_postgres(
    source_id: str,
    query: str,
    constraints: dict,
    run_id: str,
) -> dict:
    """Execute a read-only SQL query against PostgreSQL."""
    # Mutation detection
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

    try:
        import psycopg2  # noqa: PLC0415
    except ImportError:
        return {
            "status": "error",
            "error": "psycopg2 not available",
            "rows": [],
            "row_count": 0,
            "bytes_scanned": 0,
            "cost": "$0.0000",
        }

    conn = None
    try:
        params = _get_connection_params()
        conn = psycopg2.connect(
            host=params["host"],
            port=int(params.get("port", 5432)),
            dbname=params["dbname"],
            user=params["username"],
            password=params["password"],
            connect_timeout=10,
            options="-c statement_timeout=60000",  # 60s query timeout
        )
        conn.set_session(readonly=True, autocommit=True)

        with conn.cursor() as cur:
            max_rows = int(constraints.get("max_rows", 10000))
            cur.execute(query)
            columns = [desc[0] for desc in cur.description] if cur.description else []
            raw_rows = cur.fetchmany(max_rows)

        rows = [dict(zip(columns, row, strict=False)) for row in raw_rows]

        return {
            "status": "complete",
            "rows": rows,
            "schema": [{"name": c, "type": "string"} for c in columns],
            "row_count": len(rows),
            "bytes_scanned": 0,
            "cost": "$0.0000",
        }
    except Exception as exc:
        logger.error("PostgreSQL execution failed: %s", exc)
        return {
            "status": "error",
            "error": "Query execution failed",
            "rows": [],
            "row_count": 0,
            "bytes_scanned": 0,
            "cost": "$0.0000",
        }
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
