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


def _flatten_aggregations(aggs: dict) -> list[dict]:
    """Recursively flatten OpenSearch bucket aggregations into a list of row dicts.

    Handles single-level and nested terms aggregations. For each agg key that
    has a 'buckets' list, the bucket key becomes a column value. Leaf buckets
    contribute a 'count' column from doc_count.

    Example (two-level terms agg):
        {"by_service": {"buckets": [
            {"key": "payment-svc", "top_messages": {"buckets": [
                {"key": "Upstream timeout", "doc_count": 847}
            ]}}
        ]}}
    →   [{"by_service": "payment-svc", "top_messages": "Upstream timeout", "count": 847}]
    """
    bucket_aggs = {k: v for k, v in aggs.items() if isinstance(v, dict) and "buckets" in v}
    if not bucket_aggs:
        return []

    agg_name, agg_value = next(iter(bucket_aggs.items()))
    rows: list[dict] = []

    for bucket in agg_value.get("buckets", []):
        base: dict = {agg_name: bucket.get("key")}
        nested = {k: v for k, v in bucket.items() if isinstance(v, dict) and "buckets" in v}
        if nested:
            for nested_row in _flatten_aggregations(nested):
                rows.append({**base, **nested_row})
        else:
            rows.append({**base, "count": bucket.get("doc_count", 0)})

    return rows


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

    # read_only: block mutation operations in the DSL body
    if constraints.get("read_only"):
        mutation_keys = {"_delete_by_query", "_update_by_query", "_bulk"}
        body_str = json.dumps(query_body)
        for key in mutation_keys:
            if key in body_str:
                return {
                    "status": "error",
                    "error": f"read_only constraint violated: DSL contains '{key}'",
                }

    max_rows = min(int(constraints.get("max_rows", 100)), 1000)
    timeout_seconds = constraints.get("timeout_seconds", 30)
    # Only set size if the query isn't aggregation-only (size=0 means agg-only)
    if query_body.get("size", -1) != 0:
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

    # Aggregation response takes priority over hits
    aggs = response.get("aggregations", {})
    if aggs:
        rows = _flatten_aggregations(aggs)
    else:
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
