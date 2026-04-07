"""clAWS DynamoDB PartiQL executor.

source_id format: "dynamodb:TableName"
query: valid PartiQL SELECT statement

Uses execute_statement (PartiQL) — read-only by design.
Responses are unmarshalled from DynamoDB AttributeValue format.
"""


import boto3
from boto3.dynamodb.types import TypeDeserializer

_dynamodb = None
_deser = TypeDeserializer()

_MUTATION_KEYWORDS = frozenset({"INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "TRUNCATE", "ALTER"})


def _check_mutation(query: str) -> None:
    """Raise ValueError if the query is a write/mutation statement."""
    first_word = query.strip().split()[0].upper() if query.strip() else ""
    if first_word in _MUTATION_KEYWORDS:
        raise ValueError(f"Mutation query '{first_word}' not allowed — claws is read-only")


def _client():
    global _dynamodb
    if _dynamodb is None:
        _dynamodb = boto3.client("dynamodb")
    return _dynamodb


def _unmarshal(item: dict) -> dict:
    """Convert a DynamoDB AttributeValue item dict to plain Python types."""
    return {k: _deser.deserialize(v) for k, v in item.items()}


def execute_dynamodb(
    source_id: str,
    query: str,
    constraints: dict,
    run_id: str,
) -> dict:
    """Execute a PartiQL SELECT against a DynamoDB table.

    Returns:
        {"status": "complete"|"error"|"timeout", "rows": [...],
         "bytes_scanned": 0, "cost": "$0.0000"}
    """
    if not source_id.startswith("dynamodb:"):
        return {"status": "error", "error": f"Invalid dynamodb source_id: {source_id}"}

    max_rows = min(int(constraints.get("max_rows", 1000)), 1000)

    try:
        _check_mutation(query)
    except ValueError as exc:
        return {"status": "error", "error": str(exc)}

    try:
        rows: list[dict] = []
        next_token = None

        while True:
            kwargs: dict = {"Statement": query}
            if next_token:
                kwargs["NextToken"] = next_token

            resp = _client().execute_statement(**kwargs)

            for item in resp.get("Items", []):
                rows.append(_unmarshal(item))
                if len(rows) >= max_rows:
                    break

            if len(rows) >= max_rows:
                break

            next_token = resp.get("NextToken")
            if not next_token:
                break

    except Exception as exc:
        msg = str(exc)
        if "timed out" in msg.lower():
            return {"status": "timeout", "error": msg}
        return {"status": "error", "error": msg}

    return {
        "status": "complete",
        "rows": rows,
        "bytes_scanned": 0,       # DynamoDB does not expose bytes scanned per query
        "cost": "$0.0000",        # on-demand pricing not computable without capacity units
    }
