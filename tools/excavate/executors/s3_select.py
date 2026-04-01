"""S3 Select executor for clAWS excavate tool."""


def execute_s3_select(
    source_id: str,
    query: str,
    constraints: dict,
    run_id: str,
) -> dict:
    """Execute a SQL expression against S3 objects via S3 Select.

    TODO: Implement with boto3 select_object_content.
    - Parse source_id to get bucket and key/prefix
    - Determine input serialization (CSV, JSON, Parquet)
    - Execute S3 Select with scan range limits from constraints
    - Stream and collect results
    """
    return {
        "status": "error",
        "error": "S3 Select executor not yet implemented",
    }
