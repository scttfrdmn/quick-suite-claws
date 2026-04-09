"""Cost estimator for clAWS plan tool.

Estimates the cost of an excavation query before execution.
Uses table metadata (size, partitioning) and query structure
to predict bytes scanned and dollar cost.
"""

import math

# Athena pricing: $5 per TB scanned, 10 MB minimum
ATHENA_PRICE_PER_BYTE = 5.0 / (1024 ** 4)  # $5/TB
ATHENA_MIN_BYTES = 10 * 1024 * 1024  # 10 MB minimum charge

# DynamoDB on-demand pricing: $0.25 per million read request units (RRUs)
# 1 RRU = 1 strongly consistent read of up to 4 KB
_DYNAMODB_PRICE_PER_RRU = 0.25 / 1_000_000  # $0.00000025/RRU
_DYNAMODB_RRU_SIZE_BYTES = 4096  # 4 KB per RRU


def estimate_cost(source_id: str, query: str, schema: dict) -> dict:
    """Estimate the cost of running a query.

    Args:
        source_id: The qualified source identifier.
        query: The concrete query to estimate.
        schema: The cached schema from probe.

    Returns:
        {
            "estimated_bytes_scanned": int,
            "estimated_cost_dollars": float,
            "confidence": "low" | "medium" | "high",
            "notes": str,
        }
    """
    backend = source_id.split(":")[0]

    if backend == "athena":
        return _estimate_athena(query, schema)
    elif backend == "opensearch":
        return _estimate_opensearch(query, schema)
    elif backend == "s3":
        return _estimate_s3_select(query, schema)
    elif backend == "dynamodb":
        return _estimate_dynamodb(query, schema)
    elif backend == "mcp":
        return _estimate_mcp(query, schema)
    elif backend == "postgres":
        return _estimate_postgres(query, schema)
    elif backend == "redshift":
        return _estimate_redshift(query, schema)
    else:
        return {
            "estimated_bytes_scanned": 0,
            "estimated_cost_dollars": 0.0,
            "confidence": "low",
            "notes": f"No cost model for backend: {backend}",
        }


def _estimate_athena(query: str, schema: dict) -> dict:
    """Estimate Athena query cost based on table size and query structure."""
    # Base estimate: full table scan (cast away Decimal from DynamoDB deserialization)
    table_size = int(schema.get("size_bytes_estimate", 0))
    if table_size == 0:
        # Unknown size — use conservative estimate
        return {
            "estimated_bytes_scanned": ATHENA_MIN_BYTES,
            "estimated_cost_dollars": ATHENA_MIN_BYTES * ATHENA_PRICE_PER_BYTE,
            "confidence": "low",
            "notes": "Table size unknown. Using minimum charge estimate.",
        }

    estimated_bytes = table_size
    confidence = "medium"
    notes = []

    # Check for partition pruning
    partition_keys = [
        col["name"] for col in schema.get("columns", [])
        if col.get("partition_key")
    ]

    query_upper = query.upper()
    partitions_used = []
    for pk in partition_keys:
        if pk.upper() in query_upper:
            partitions_used.append(pk)

    if partitions_used:
        # Rough heuristic: each partition key in WHERE reduces scan by ~90%
        reduction = 0.1 ** len(partitions_used)
        estimated_bytes = max(int(table_size * reduction), ATHENA_MIN_BYTES)
        confidence = "medium"
        notes.append(f"Partition pruning on: {', '.join(partitions_used)}")

    # Check for columnar format (Parquet/ORC) — only scans referenced columns
    fmt = schema.get("format", "").lower()
    if "parquet" in fmt or "orc" in fmt:
        # Count columns referenced vs total
        total_cols = len(schema.get("columns", []))
        if total_cols > 0:
            # Very rough: assume SELECT references ~30% of columns on average
            col_ratio = 0.3
            estimated_bytes = int(estimated_bytes * col_ratio)
            notes.append(f"Columnar format ({fmt}) — reduced by column pruning")

    # Apply minimum
    estimated_bytes = max(estimated_bytes, ATHENA_MIN_BYTES)
    estimated_cost = estimated_bytes * ATHENA_PRICE_PER_BYTE

    return {
        "estimated_bytes_scanned": estimated_bytes,
        "estimated_cost_dollars": round(estimated_cost, 4),
        "confidence": confidence,
        "notes": "; ".join(notes) if notes else "Full scan estimate",
    }


def _estimate_opensearch(query: str, schema: dict) -> dict:
    """Estimate OpenSearch query cost. OpenSearch is provisioned,
    so per-query cost is effectively zero (covered by instance hours)."""
    return {
        "estimated_bytes_scanned": 0,
        "estimated_cost_dollars": 0.0,
        "confidence": "high",
        "notes": "OpenSearch queries have no per-query cost (provisioned).",
    }


def _estimate_s3_select(query: str, schema: dict) -> dict:
    """Estimate S3 Select cost."""
    table_size = schema.get("size_bytes_estimate", 0)
    # S3 Select: $0.002 per GB scanned, $0.0007 per GB returned
    scanned_cost = table_size / (1024 ** 3) * 0.002
    return {
        "estimated_bytes_scanned": table_size,
        "estimated_cost_dollars": round(scanned_cost, 4),
        "confidence": "medium",
        "notes": "S3 Select pricing: $0.002/GB scanned + $0.0007/GB returned.",
    }


def _estimate_dynamodb(query: str, schema: dict) -> dict:
    """Estimate DynamoDB PartiQL query cost.

    DynamoDB on-demand pricing: $0.25 per million read request units.
    1 RRU covers one strongly consistent read of up to 4 KB.
    For scan-type PartiQL queries (no KeyConditionExpression pushed down),
    the full table is read. Filtered queries may read significantly less,
    but the DynamoDB query planner does not expose a cost estimate pre-execution.

    Confidence is always "low" because:
    - PartiQL filter predicates may or may not be pushed down to the key condition
    - GSI access patterns significantly affect scan scope
    - Table size metadata may be stale (DynamoDB updates statistics every 6 hours)
    """
    row_count = int(schema.get("row_count_estimate", 10_000))
    # avg_item_size_bytes may be in schema or fall back to 512 bytes
    avg_bytes = int(schema.get("avg_item_size_bytes", 512))
    total_bytes = row_count * avg_bytes
    rrus = math.ceil(total_bytes / _DYNAMODB_RRU_SIZE_BYTES)
    cost = rrus * _DYNAMODB_PRICE_PER_RRU
    return {
        "estimated_bytes_scanned": total_bytes,
        "estimated_cost_dollars": round(cost, 6),
        "confidence": "low",
        "notes": (
            f"DynamoDB on-demand: ~{rrus:,} RRUs estimated (full scan, "
            f"{row_count:,} rows × {avg_bytes} bytes avg). "
            "Filtered queries with key conditions will cost less."
        ),
    }


def _estimate_mcp(query: str, schema: dict) -> dict:
    """Estimate MCP tool call cost.

    MCP tool calls invoke an external server. There is no AWS-side per-call charge
    beyond the Lambda execution time (covered by Lambda pricing). The cost of the
    MCP server itself depends on the server provider's pricing and is outside AWS.
    """
    return {
        "estimated_bytes_scanned": 0,
        "estimated_cost_dollars": 0.0,
        "confidence": "high",
        "notes": (
            "MCP tool calls have no per-call AWS cost. "
            "External MCP server pricing depends on the server provider."
        ),
    }


def _estimate_postgres(query: str, schema: dict) -> dict:
    """Estimate PostgreSQL query cost.

    PostgreSQL (RDS/Aurora) is provisioned — queries have no per-query
    billing. Cost is covered by instance hours.
    """
    return {
        "estimated_bytes_scanned": 0,
        "estimated_cost_dollars": 0.0,
        "confidence": "high",
        "notes": "PostgreSQL has no per-query billing",
    }


def _estimate_redshift(query: str, schema: dict) -> dict:
    """Estimate Redshift Serverless query cost using the same $5/TB model as Athena."""
    table_size = int(schema.get("size_bytes_estimate", 0))
    if table_size == 0:
        return {
            "estimated_bytes_scanned": ATHENA_MIN_BYTES,
            "estimated_cost_dollars": ATHENA_MIN_BYTES * ATHENA_PRICE_PER_BYTE,
            "confidence": "low",
            "notes": "Table size unknown. Using minimum charge estimate.",
        }

    estimated_bytes = table_size
    confidence = "medium"
    notes = []

    # Check for partition pruning (sort keys in Redshift)
    partition_keys = [
        col["name"] for col in schema.get("columns", [])
        if col.get("partition_key") or col.get("sort_key")
    ]

    query_upper = query.upper()
    partitions_used = []
    for pk in partition_keys:
        if pk.upper() in query_upper:
            partitions_used.append(pk)

    if partitions_used:
        reduction = 0.1 ** len(partitions_used)
        estimated_bytes = max(int(table_size * reduction), ATHENA_MIN_BYTES)
        notes.append(f"Sort key pruning on: {', '.join(partitions_used)}")

    # Columnar storage — Redshift always uses columnar
    total_cols = len(schema.get("columns", []))
    if total_cols > 0:
        col_ratio = 0.3
        estimated_bytes = int(estimated_bytes * col_ratio)
        notes.append("Columnar storage — reduced by column pruning")

    estimated_bytes = max(estimated_bytes, ATHENA_MIN_BYTES)
    estimated_cost = estimated_bytes * ATHENA_PRICE_PER_BYTE

    return {
        "estimated_bytes_scanned": estimated_bytes,
        "estimated_cost_dollars": round(estimated_cost, 4),
        "confidence": confidence,
        "notes": "; ".join(notes) if notes else "Full scan estimate",
    }
