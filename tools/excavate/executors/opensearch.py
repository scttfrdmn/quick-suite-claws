"""OpenSearch executor for clAWS excavate tool.

Executes OpenSearch DSL queries against an AWS OpenSearch Service domain
using SigV4 request signing (requests-aws4auth).

Source ID format:  opensearch:endpoint/index
Example:           opensearch:search-prod.us-east-1.es.amazonaws.com/logs-2024

Substrate does not support OpenSearch — tests mock _os_client() directly.
"""

import json
import os
from typing import Any

import boto3

# Cached OpenSearch clients keyed by endpoint — one per domain per Lambda lifetime
OS_CLIENT: dict[str, Any] = {}


def _parse_source_id(source_id: str) -> tuple[str, str]:
    """Split 'opensearch:endpoint/index' into (endpoint, index).

    Raises ValueError if the format is not recognized.
    """
    _, _, remainder = source_id.partition(":")
    if not remainder or "/" not in remainder:
        raise ValueError(
            f"Invalid opensearch source_id '{source_id}'. "
            "Expected format: opensearch:endpoint/index"
        )
    endpoint, _, index = remainder.partition("/")
    if not endpoint or not index:
        raise ValueError(
            f"Missing endpoint or index in opensearch source_id '{source_id}'"
        )
    return endpoint, index


def _os_client(endpoint: str) -> Any:
    """Return a cached OpenSearch client for the given endpoint.

    Uses SigV4 signing via requests-aws4auth. The region is read from
    AWS_DEFAULT_REGION (default: us-east-1).
    """
    if endpoint in OS_CLIENT:
        return OS_CLIENT[endpoint]

    from opensearchpy import OpenSearch, RequestsHttpConnection  # type: ignore[import]
    from requests_aws4auth import AWS4Auth  # type: ignore[import]

    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    credentials = boto3.Session().get_credentials()
    auth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        region,
        "es",
        session_token=credentials.token,
    )
    client = OpenSearch(
        hosts=[{"host": endpoint, "port": 443}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        timeout=30,
    )
    OS_CLIENT[endpoint] = client
    return client


def execute_opensearch(
    source_id: str,
    query: str | dict,
    constraints: dict,
    run_id: str,
) -> dict:
    """Execute a DSL query against an OpenSearch domain.

    Args:
        source_id: Source identifier, e.g.
            "opensearch:search-prod.us-east-1.es.amazonaws.com/logs"
        query: OpenSearch DSL query body as a JSON string or dict.
        constraints: Optional keys:
            - max_rows (int): maps to DSL 'size', capped at 1000 (default 100)
            - timeout_seconds (int): query timeout in seconds (default 30)
        run_id: clAWS run identifier (reserved for tracing)

    Returns:
        {"status": "complete"|"error"|"timeout", "rows": [...],
         "bytes_scanned": 0, "cost": "$0.0000"}
    """
    try:
        endpoint, index = _parse_source_id(source_id)
    except ValueError as e:
        return {"status": "error", "error": str(e)}

    if isinstance(query, str):
        try:
            query_body: dict = json.loads(query)
        except json.JSONDecodeError as e:
            return {"status": "error", "error": f"query is not valid JSON: {e}"}
    elif isinstance(query, dict):
        query_body = query
    else:
        return {
            "status": "error",
            "error": f"query must be a JSON string or dict, got {type(query).__name__}",
        }

    max_rows = min(int(constraints.get("max_rows", 100)), 1000)
    timeout_seconds = constraints.get("timeout_seconds", 30)
    query_body = {**query_body, "size": max_rows}

    try:
        client = _os_client(endpoint)
        response = client.search(
            index=index,
            body=query_body,
            request_timeout=timeout_seconds,
        )
    except Exception as e:
        err_lower = str(e).lower()
        if "timed out" in err_lower or "timeout" in err_lower:
            return {
                "status": "timeout",
                "error": f"OpenSearch query timed out after {timeout_seconds}s",
            }
        return {"status": "error", "error": f"OpenSearch search failed: {e}"}

    rows = [
        hit.get("_source", {})
        for hit in response.get("hits", {}).get("hits", [])
    ]
    return {
        "status": "complete",
        "rows": rows,
        "bytes_scanned": 0,
        "cost": "$0.0000",
    }
