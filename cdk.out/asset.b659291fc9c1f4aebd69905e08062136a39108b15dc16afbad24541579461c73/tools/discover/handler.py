"""clAWS discover tool — find data sources matching a topic."""

import json
import os
from typing import Any

import boto3

from tools.shared import audit_log, error, success

# Source registry backends
GLUE_CLIENT = None
OPENSEARCH_CLIENT = None
S3_CLIENT = None
_DYNAMODB_RESOURCE = None
_SSM_CLIENT = None

# Data source registry table name — resolved lazily from SSM or env override
_DATA_SOURCE_REGISTRY_TABLE: str | None = None


def glue_client() -> Any:
    global GLUE_CLIENT
    if GLUE_CLIENT is None:
        GLUE_CLIENT = boto3.client("glue")
    return GLUE_CLIENT


def _s3_client() -> Any:
    global S3_CLIENT
    if S3_CLIENT is None:
        S3_CLIENT = boto3.client("s3")
    return S3_CLIENT


def _dynamodb_resource() -> Any:
    global _DYNAMODB_RESOURCE
    if _DYNAMODB_RESOURCE is None:
        _DYNAMODB_RESOURCE = boto3.resource("dynamodb")
    return _DYNAMODB_RESOURCE


def _ssm_client() -> Any:
    global _SSM_CLIENT
    if _SSM_CLIENT is None:
        _SSM_CLIENT = boto3.client("ssm")
    return _SSM_CLIENT


def _registry_table_name() -> str | None:
    """Return the data source registry DynamoDB table name.

    Checks DATA_SOURCE_REGISTRY_TABLE env var first (set by CDK / test monkeypatch).
    Falls back to reading SSM param /quick-suite/data/source-registry-arn, which
    contains the table ARN; extracts the table name from the ARN.
    Returns None if neither is configured.
    """
    global _DATA_SOURCE_REGISTRY_TABLE
    if _DATA_SOURCE_REGISTRY_TABLE is not None:
        return _DATA_SOURCE_REGISTRY_TABLE

    from_env = os.environ.get("DATA_SOURCE_REGISTRY_TABLE", "")
    if from_env:
        _DATA_SOURCE_REGISTRY_TABLE = from_env
        return _DATA_SOURCE_REGISTRY_TABLE

    try:
        resp = _ssm_client().get_parameter(Name="/quick-suite/data/source-registry-arn")
        arn = resp["Parameter"]["Value"]
        # ARN format: arn:aws:dynamodb:<region>:<account>:table/<table-name>
        table_name = arn.split("/")[-1]
        _DATA_SOURCE_REGISTRY_TABLE = table_name
        return _DATA_SOURCE_REGISTRY_TABLE
    except Exception as e:
        print(f"Could not load source registry table from SSM: {e}")
        return None


def handler(event: dict, context: Any) -> dict:
    """Lambda handler for claws.discover.

    Searches Glue Data Catalog, OpenSearch domains, and S3 inventory
    for sources matching the query within approved scope.
    """
    body = json.loads(event.get("body", "{}")) if isinstance(event.get("body"), str) else event
    query = body.get("query", "")
    scope = body.get("scope", {})
    limit = body.get("limit", 10)
    principal = event.get("requestContext", {}).get("authorizer", {}).get("principalId", "unknown")
    request_id = event.get("requestContext", {}).get("requestId", "")

    if not query:
        return error("query is required")

    domains = scope.get("domains", [])
    spaces = scope.get("spaces", [])

    sources = []

    # Search Glue Data Catalog for Athena tables
    if "athena" in domains:
        sources.extend(_discover_glue(query, spaces, limit))

    # Search OpenSearch domains
    if "opensearch" in domains:
        sources.extend(_discover_opensearch(query, spaces, limit))

    # Search S3 inventory
    if "s3" in domains:
        sources.extend(_discover_s3(query, spaces, limit))

    # Search registered MCP servers
    if "mcp" in domains:
        sources.extend(_discover_mcp(query, spaces, limit))

    # Search quick-suite-data source registry
    if "registry" in domains:
        sources.extend(_discover_registry(query, limit))

    # Sort by confidence, apply limit
    sources.sort(key=lambda s: s.get("confidence", 0), reverse=True)
    sources = sources[:limit]

    result = {"sources": sources}

    audit_log("discover", principal, body, {"source_count": len(sources)}, request_id=request_id)

    return success(result)


def _discover_glue(query: str, spaces: list[str], limit: int) -> list[dict]:
    """Search Glue Data Catalog databases and tables."""
    sources = []
    try:
        # List databases matching spaces
        response = glue_client().get_databases()
        for db in response.get("DatabaseList", []):
            db_name = db["Name"]

            # Filter by space membership
            tags = db.get("Parameters", {})
            db_space = tags.get("claws:space", "default")
            if spaces and db_space not in spaces:
                continue

            # Search tables within matching databases
            tables_resp = glue_client().get_tables(
                DatabaseName=db_name,
                Expression=f"*{query.split()[0]}*" if query else "*",
                MaxResults=min(limit, 20),
            )

            for table in tables_resp.get("TableList", []):
                # Simple relevance scoring based on name/description match
                name = table["Name"].lower()
                desc = table.get("Description", "").lower()
                query_lower = query.lower()

                score = 0.0
                for term in query_lower.split():
                    if term in name:
                        score += 0.4
                    if term in desc:
                        score += 0.3

                if score > 0:
                    sources.append({
                        "id": f"athena:{db_name}.{table['Name']}",
                        "kind": "table",
                        "confidence": min(score, 1.0),
                        "reason": (
                            "Matches query in "
                            + ("name" if query_lower.split()[0] in name else "description")
                        ),
                    })

    except Exception as e:
        print(f"Glue discovery error: {e}")

    return sources


def _discover_opensearch(query: str, spaces: list[str], limit: int) -> list[dict]:
    """Search OpenSearch domain indices.

    spaces = list of OpenSearch endpoints, e.g. "search-prod.us-east-1.es.amazonaws.com"
    """
    from tools.excavate.executors.opensearch import _os_client  # noqa: PLC0415

    sources: list[dict] = []
    query_terms = query.lower().split()

    for endpoint in spaces:
        try:
            client = _os_client(endpoint)
            indices = client.cat.indices(format="json") or []
            for entry in indices:
                index_name = entry.get("index", "")
                score = sum(0.4 for term in query_terms if term in index_name.lower())
                if score > 0:
                    sources.append({
                        "id": f"opensearch:{endpoint}/{index_name}",
                        "kind": "index",
                        "confidence": min(score, 1.0),
                        "reason": "Matches query in index name",
                    })
        except Exception as e:
            print(f"OpenSearch discovery error for {endpoint}: {e}")

    return sources


def _discover_s3(query: str, spaces: list[str], limit: int) -> list[dict]:
    """Search S3 buckets for matching common prefixes or object keys.

    spaces = list of S3 bucket names.
    """
    sources: list[dict] = []
    query_terms = query.lower().split()

    for bucket in spaces:
        try:
            response = _s3_client().list_objects_v2(
                Bucket=bucket, Delimiter="/", MaxKeys=100
            )

            # Score common prefixes (folder-level discovery)
            for cp in response.get("CommonPrefixes", []):
                prefix = cp.get("Prefix", "")
                score = sum(0.4 for term in query_terms if term in prefix.lower())
                if score > 0:
                    sources.append({
                        "id": f"s3://{bucket}/{prefix}",
                        "kind": "prefix",
                        "confidence": min(score, 1.0),
                        "reason": "Matches query in S3 prefix",
                    })

            # Fall back to object keys if no matching prefixes
            if not any(s["id"].startswith(f"s3://{bucket}/") for s in sources):
                for obj in response.get("Contents", []):
                    key = obj.get("Key", "")
                    score = sum(0.4 for term in query_terms if term in key.lower())
                    if score > 0:
                        sources.append({
                            "id": f"s3://{bucket}/{key}",
                            "kind": "object",
                            "confidence": min(score, 1.0),
                            "reason": "Matches query in S3 object key",
                        })

        except Exception as e:
            print(f"S3 discovery error for bucket {bucket}: {e}")

    return sources


def _discover_mcp(query: str, spaces: list[str], limit: int) -> list[dict]:
    """Discover resources from registered MCP servers.

    spaces = list of server names to restrict discovery to (empty = all servers).
    Each server's resources/list is called and scored against query terms.
    Errors per server are caught and skipped — same pattern as _discover_opensearch.
    """
    from tools.mcp.client import run_mcp_async  # noqa: PLC0415
    from tools.mcp.registry import get_mcp_registry  # noqa: PLC0415

    registry = get_mcp_registry()
    if not registry:
        return []

    sources: list[dict] = []
    query_terms = query.lower().split()

    # Apply spaces filter (server names), or use all registered servers
    server_names = [s for s in registry if not spaces or s in spaces]

    for server_name in server_names:
        server_config = registry[server_name]
        try:
            async def _list_resources(session):  # noqa: E306
                result = await session.list_resources()
                return result.resources

            resources = run_mcp_async(_list_resources, server_config)

            for resource in resources:
                name = resource.name.lower()
                desc = (resource.description or "").lower()
                uri_str = str(resource.uri).lower()

                score = 0.0
                for term in query_terms:
                    if term in name or term in uri_str:
                        score += 0.4
                    if term in desc:
                        score += 0.3

                if score > 0:
                    sources.append({
                        "id": f"mcp://{server_name}/{resource.name}",
                        "kind": "mcp_resource",
                        "confidence": min(score, 1.0),
                        "reason": "Matches query in MCP resource name or description",
                    })

        except Exception as e:
            print(f"MCP discovery error for server '{server_name}': {e}")

    return sources


def _discover_registry(query: str, limit: int) -> list[dict]:
    """Search the quick-suite-data source registry DynamoDB table.

    The table is populated by quick-suite-data's register-source Lambda and
    contains entries from roda_load and s3_load operations. Each item has at
    minimum: source_id (PK), source_type, name/description, data_classification,
    and quality_score fields.

    Table name comes from DATA_SOURCE_REGISTRY_TABLE env var or SSM param
    /quick-suite/data/source-registry-arn (ARN → table name extraction).
    """
    table_name = _registry_table_name()
    if not table_name:
        print("Registry domain requested but no table configured — skipping")
        return []

    sources: list[dict] = []
    query_terms = query.lower().split()

    try:
        table = _dynamodb_resource().Table(table_name)
        resp = table.scan()
        items = resp.get("Items", [])

        for item in items:
            name = str(item.get("name", item.get("source_id", ""))).lower()
            desc = str(item.get("description", "")).lower()
            tags = " ".join(str(t) for t in item.get("tags", [])).lower()

            score = 0.0
            for term in query_terms:
                if term in name:
                    score += 0.4
                if term in desc:
                    score += 0.3
                if term in tags:
                    score += 0.2

            if score > 0:
                entry: dict = {
                    "id": item.get("source_id", ""),
                    "kind": "registry",
                    "confidence": min(score, 1.0),
                    "reason": "Matches query in registry name or description",
                }
                if "data_classification" in item:
                    entry["data_classification"] = item["data_classification"]
                if "quality_score" in item:
                    entry["quality_score"] = float(item["quality_score"])
                sources.append(entry)

    except Exception as e:
        print(f"Registry discovery error: {e}")

    return sources
