"""clAWS probe tool — inspect a discovered source."""

import json

import boto3

from tools.shared import (
    audit_log, cache_schema, scan_payload, success, error,
)


GLUE_CLIENT = None
ATHENA_CLIENT = None


def glue_client():
    global GLUE_CLIENT
    if GLUE_CLIENT is None:
        GLUE_CLIENT = boto3.client("glue")
    return GLUE_CLIENT


def athena_client():
    global ATHENA_CLIENT
    if ATHENA_CLIENT is None:
        ATHENA_CLIENT = boto3.client("athena")
    return ATHENA_CLIENT


def handler(event, context):
    """Lambda handler for claws.probe.

    Inspects a source: schema, sample rows, size estimates, cost estimates.
    Sample data is scanned via ApplyGuardrail for PII/PHI before return.
    """
    body = json.loads(event.get("body", "{}")) if isinstance(event.get("body"), str) else event
    source_id = body.get("source_id", "")
    mode = body.get("mode", "schema_only")
    sample_rows = body.get("sample_rows", 5)
    principal = event.get("requestContext", {}).get("authorizer", {}).get("principalId", "unknown")

    if not source_id:
        return error("source_id is required")

    # Parse source_id: "athena:database.table" or "opensearch:index" etc.
    backend, _, qualified_name = source_id.partition(":")
    if not qualified_name:
        return error(f"Invalid source_id format: {source_id}")

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
    })

    return success(result)


def _probe_athena(qualified_name: str, mode: str, sample_rows: int) -> dict:
    """Probe an Athena table via Glue Data Catalog."""
    parts = qualified_name.split(".", 1)
    if len(parts) != 2:
        return {"error": f"Expected database.table, got: {qualified_name}"}

    database, table_name = parts
    result = {}

    try:
        # Get schema from Glue
        table = glue_client().get_table(DatabaseName=database, Name=table_name)
        table_data = table["Table"]

        columns = []
        for col in table_data.get("StorageDescriptor", {}).get("Columns", []):
            columns.append({
                "name": col["Name"],
                "type": col["Type"],
                "comment": col.get("Comment", ""),
            })
        for col in table_data.get("PartitionKeys", []):
            columns.append({
                "name": col["Name"],
                "type": col["Type"],
                "comment": col.get("Comment", ""),
                "partition_key": True,
            })

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
    """Run a LIMIT query to get sample rows."""
    # TODO: Use Athena StartQueryExecution with read-only workgroup
    # For now, return placeholder indicating the integration point
    return []


def _probe_opensearch(qualified_name: str, mode: str, sample_rows: int) -> dict:
    """Probe an OpenSearch index."""
    # TODO: Implement OpenSearch index probe
    return {"error": "OpenSearch probe not yet implemented"}
