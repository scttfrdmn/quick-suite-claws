"""clAWS discover tool — find data sources matching a topic."""

import json
from typing import Any

import boto3

from tools.shared import audit_log, error, success

# Source registry backends
GLUE_CLIENT = None
OPENSEARCH_CLIENT = None


def glue_client() -> Any:
    global GLUE_CLIENT
    if GLUE_CLIENT is None:
        GLUE_CLIENT = boto3.client("glue")
    return GLUE_CLIENT


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

    # Sort by confidence, apply limit
    sources.sort(key=lambda s: s.get("confidence", 0), reverse=True)
    sources = sources[:limit]

    result = {"sources": sources}

    audit_log("discover", principal, body, {"source_count": len(sources)})

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
    """Search OpenSearch domain indices."""
    # TODO: Implement OpenSearch index discovery
    return []


def _discover_s3(query: str, spaces: list[str], limit: int) -> list[dict]:
    """Search S3 inventory for matching prefixes/objects."""
    # TODO: Implement S3 inventory search
    return []
