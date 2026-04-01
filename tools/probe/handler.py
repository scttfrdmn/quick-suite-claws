"""clAWS probe tool — inspect a discovered source."""

import json
import os
import time
from typing import Any

import boto3

from tools.errors import ValidationError
from tools.shared import (
    audit_log,
    cache_schema,
    error,
    scan_payload,
    success,
)

GLUE_CLIENT = None
ATHENA_CLIENT = None

WORKGROUP = os.environ.get("CLAWS_ATHENA_WORKGROUP", "claws-readonly")
OUTPUT_LOCATION = os.environ.get("CLAWS_ATHENA_OUTPUT", "s3://claws-athena-results/")


def glue_client() -> Any:
    global GLUE_CLIENT
    if GLUE_CLIENT is None:
        GLUE_CLIENT = boto3.client("glue")
    return GLUE_CLIENT


def athena_client() -> Any:
    global ATHENA_CLIENT
    if ATHENA_CLIENT is None:
        ATHENA_CLIENT = boto3.client("athena")
    return ATHENA_CLIENT


def handler(event: dict, context: Any) -> dict:
    """Lambda handler for claws.probe.

    Inspects a source: schema, sample rows, size estimates, cost estimates.
    Sample data is scanned via ApplyGuardrail for PII/PHI before return.
    """
    body = json.loads(event.get("body", "{}")) if isinstance(event.get("body"), str) else event
    source_id = body.get("source_id", "")
    mode = body.get("mode", "schema_only")
    sample_rows = body.get("sample_rows", 5)
    principal = event.get("requestContext", {}).get("authorizer", {}).get("principalId", "unknown")
    request_id = event.get("requestContext", {}).get("requestId", "")

    if not source_id:
        return error("source_id is required")

    # Parse source_id: "athena:database.table" or "opensearch:index" etc.
    backend, _, qualified_name = source_id.partition(":")
    if not qualified_name:
        return error(ValidationError(f"Invalid source_id format: {source_id}"))

    result = {"source_id": source_id}

    if backend == "athena":
        result.update(_probe_athena(qualified_name, mode, sample_rows))
    elif backend == "opensearch":
        result.update(_probe_opensearch(qualified_name, mode, sample_rows))
    else:
        return error(f"Unsupported backend: {backend}")

    # Cache schema for use by the plan tool
    if "schema" in result:
        cache_schema(source_id, result["schema"])

    # Scan sample data for PII/PHI before returning to agent
    if "samples" in result and result["samples"]:
        scan_result = scan_payload(result["samples"])
        if scan_result["status"] == "blocked":
            result["samples"] = []
            result["sample_warning"] = "Samples redacted: sensitive content detected"

    audit_log("probe", principal, body, {
        "schema_columns": len(result.get("schema", {}).get("columns", [])),
        "samples_returned": len(result.get("samples", [])),
    }, request_id=request_id)

    return success(result)


def _probe_athena(qualified_name: str, mode: str, sample_rows: int) -> dict:
    """Probe an Athena table via Glue Data Catalog."""
    parts = qualified_name.split(".", 1)
    if len(parts) != 2:
        return {"error": f"Expected database.table, got: {qualified_name}"}

    database, table_name = parts
    result: dict = {}

    try:
        # Get schema from Glue
        table = glue_client().get_table(DatabaseName=database, Name=table_name)
        table_data = table["Table"]

        columns = []
        for col in table_data.get("StorageDescriptor", {}).get("Columns", []):
            entry: dict = {"name": col["Name"], "type": col["Type"]}
            if comment := col.get("Comment", ""):
                entry["comment"] = comment
            columns.append(entry)
        for col in table_data.get("PartitionKeys", []):
            entry = {"name": col["Name"], "type": col["Type"], "partition_key": True}
            if comment := col.get("Comment", ""):
                entry["comment"] = comment
            columns.append(entry)

        result["schema"] = {
            "database": database,
            "table": table_name,
            "columns": columns,
            "format": table_data.get("StorageDescriptor", {}).get("InputFormat", ""),
            "location": table_data.get("StorageDescriptor", {}).get("Location", ""),
        }

        # Size estimate from table parameters
        params = table_data.get("Parameters", {})
        if "recordCount" in params:
            result["row_count_estimate"] = int(params["recordCount"])
        if "averageRecordSize" in params and "recordCount" in params:
            result["size_bytes_estimate"] = (
                int(params["averageRecordSize"]) * int(params["recordCount"])
            )

        # Sample rows if requested
        if mode in ("schema_and_samples", "full") and sample_rows > 0:
            result["samples"] = _sample_athena(database, table_name, sample_rows)

        # Cost estimates
        if mode in ("cost_estimate", "full"):
            size = result.get("size_bytes_estimate", 0)
            result["cost_estimates"] = {
                "full_scan_bytes": size,
                "full_scan_cost": f"${size / 1e12 * 5:.4f}",  # $5/TB scanned
                "note": "Athena charges $5 per TB scanned. Partitioned queries cost less.",
            }

    except Exception as e:
        result["error"] = str(e)

    return result


def _sample_athena(database: str, table_name: str, limit: int) -> list[dict]:
    """Run a LIMIT query against Athena to get sample rows.

    Uses the read-only workgroup. Returns an empty list on any failure
    so that probe never fails hard due to sampling errors.
    """
    try:
        response = athena_client().start_query_execution(
            QueryString=f"SELECT * FROM {database}.{table_name} LIMIT {limit}",
            WorkGroup=WORKGROUP,
            ResultConfiguration={
                "OutputLocation": f"{OUTPUT_LOCATION}_probe/",
            },
        )
        execution_id = response["QueryExecutionId"]

        # Poll for completion (30s timeout)
        start_time = time.time()
        while True:
            if time.time() - start_time > 30:
                import contextlib
                with contextlib.suppress(Exception):
                    athena_client().stop_query_execution(QueryExecutionId=execution_id)
                return []

            status_resp = athena_client().get_query_execution(QueryExecutionId=execution_id)
            state = status_resp["QueryExecution"]["Status"]["State"]

            if state == "SUCCEEDED":
                break
            elif state in ("FAILED", "CANCELLED"):
                return []

            time.sleep(1)

        # Fetch results, skip header row
        rows: list[dict] = []
        paginator = athena_client().get_paginator("get_query_results")
        columns = None
        for page in paginator.paginate(QueryExecutionId=execution_id):
            result_set = page["ResultSet"]
            if columns is None:
                columns = [
                    col["Name"]
                    for col in result_set["ResultSetMetadata"]["ColumnInfo"]
                ]
            for row in result_set["Rows"]:
                values = [datum.get("VarCharValue", "") for datum in row["Data"]]
                if values == columns:  # skip header row
                    continue
                rows.append(dict(zip(columns, values, strict=False)))

        return rows

    except Exception:
        return []


def _probe_opensearch(qualified_name: str, mode: str, sample_rows: int) -> dict:
    """Probe an OpenSearch index: schema (mapping), stats, and optional samples."""
    from tools.excavate.executors.opensearch import _os_client, _parse_source_id  # noqa: PLC0415

    try:
        endpoint, index = _parse_source_id(f"opensearch:{qualified_name}")
    except ValueError as e:
        return {"error": str(e)}

    client = _os_client(endpoint)
    result: dict = {}

    try:
        # Schema from index mapping
        mapping = client.indices.get_mapping(index=index)
        properties = (
            mapping.get(index, {}).get("mappings", {}).get("properties", {})
        )
        columns = [{"name": k, "type": v.get("type", "object")} for k, v in properties.items()]
        result["schema"] = {"index": index, "endpoint": endpoint, "columns": columns}

        # Stats
        stats = client.indices.stats(index=index)
        idx_stats = stats.get("indices", {}).get(index, {}).get("total", {})
        result["row_count_estimate"] = idx_stats.get("docs", {}).get("count", 0)
        result["size_bytes_estimate"] = idx_stats.get("store", {}).get("size_in_bytes", 0)

        # Samples
        if mode in ("schema_and_samples", "full") and sample_rows > 0:
            resp = client.search(
                index=index,
                body={"query": {"match_all": {}}, "size": sample_rows},
            )
            result["samples"] = [
                hit.get("_source", {})
                for hit in resp.get("hits", {}).get("hits", [])
            ]

        # Cost estimates
        if mode in ("cost_estimate", "full"):
            result["cost_estimates"] = {"note": "OpenSearch charges no per-query scan fee."}

    except Exception as e:
        result["error"] = str(e)

    return result
