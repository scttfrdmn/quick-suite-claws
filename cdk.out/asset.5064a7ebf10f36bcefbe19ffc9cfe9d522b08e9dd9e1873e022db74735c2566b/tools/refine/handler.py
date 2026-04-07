"""clAWS refine tool — post-process excavation results.

Operations: dedupe, rank, filter, summarize, normalize, merge.
The summarize operation uses Bedrock with guardrail for
contextual grounding checks.
The merge operation combines an existing dataset (result_s3_uri) with new
excavation results, deduplicating by a configurable key field.
"""

import json
import os
from typing import Any

from tools.shared import (
    GUARDRAIL_ID,
    GUARDRAIL_VERSION,
    audit_log,
    bedrock_runtime,
    call_router,
    error,
    load_result,
    new_run_id,
    s3_client,
    scan_payload,
    store_result,
    success,
)

MODEL_ID = os.environ.get("CLAWS_REFINE_MODEL_ID", "anthropic.claude-sonnet-4-20250514-v1:0")

ALLOWED_OPERATIONS = {"dedupe", "rank", "rank_by_n", "filter", "summarize", "normalize", "merge"}


def handler(event: dict, context: Any) -> dict:
    """Lambda handler for claws.refine."""
    body = json.loads(event.get("body", "{}")) if isinstance(event.get("body"), str) else event
    run_id = body.get("run_id", "")
    operations = body.get("operations", [])
    top_k = body.get("top_k", 25)
    mode = body.get("mode", "")
    principal = event.get("requestContext", {}).get("authorizer", {}).get("principalId", "unknown")
    request_id = event.get("requestContext", {}).get("requestId", "")

    # Handle merge mode separately — it takes result_s3_uri + new run_id
    if mode == "merge":
        return _handle_merge(body, principal, request_id)

    if not run_id:
        return error("run_id is required")
    if not operations:
        return error("operations is required")

    # Validate operations — rank_by_<field> is a valid rank variant;
    # dict ops (e.g. {"op": "filter", ...}) are resolved by their "op" key
    def _op_name(op: Any) -> str:
        return op if isinstance(op, str) else op.get("op", "")

    invalid = [
        op for op in operations
        if _op_name(op) not in ALLOWED_OPERATIONS and not _op_name(op).startswith("rank_by_")
    ]
    if invalid:
        return error(f"Invalid operations: {invalid}. Allowed: {ALLOWED_OPERATIONS}")

    # Load source results
    try:
        rows = load_result(run_id)
    except Exception as e:
        return error(f"Failed to load results for {run_id}: {e}", status_code=404)

    manifest = {"source_run_id": run_id, "source_rows": len(rows), "operations": []}

    # Apply operations in order
    for op in operations:
        before_count = len(rows) if isinstance(rows, list) else 0
        op_name = op if isinstance(op, str) else op.get("op", "")

        if op_name == "dedupe":
            rows = _dedupe(rows)
        elif op_name.startswith("rank"):
            rows = _rank(rows, op_name)
        elif op_name == "filter" and isinstance(op, dict):
            rows = _filter(rows, op)
        elif op_name == "normalize":
            rows = _normalize(rows)
        elif op_name == "summarize":
            rows = _summarize(rows, run_id, top_k)

        after_count = len(rows) if isinstance(rows, list) else 1
        manifest["operations"].append({
            "operation": op_name,
            "rows_before": before_count,
            "rows_after": after_count,
        })

    # Apply top_k
    if isinstance(rows, list) and len(rows) > top_k:
        rows = rows[:top_k]
        manifest["top_k_applied"] = top_k

    # Store refined results
    refined_run_id = new_run_id()
    result_uri = store_result(refined_run_id, rows)

    # Scan refined output
    scan_result = scan_payload(rows)
    if scan_result["status"] == "blocked":
        return success({
            "run_id": refined_run_id,
            "status": "blocked",
            "reason": "Refined results contain sensitive content",
        })

    audit_log("refine", principal, body, {
        "refined_run_id": refined_run_id,
        "manifest": manifest,
    }, request_id=request_id)

    return success({
        "run_id": refined_run_id,
        "refined_uri": result_uri,
        "manifest": manifest,
    })


def _load_s3_uri(uri: str) -> list[dict]:
    """Load an NDJSON or JSON array from an arbitrary S3 URI."""
    parts = uri.replace("s3://", "").split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    obj = s3_client().get_object(Bucket=bucket, Key=key)
    raw = obj["Body"].read().decode("utf-8").strip()
    # Try JSON array first, then NDJSON
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            return data
        return [data]
    except json.JSONDecodeError:
        return [json.loads(line) for line in raw.splitlines() if line.strip()]


def _handle_merge(body: dict, principal: str, request_id: str) -> dict:
    """Merge mode: load existing dataset from result_s3_uri, merge with new run results.

    Deduplicates by dedup_key field. Writes merged result back to S3 at
    output_s3_uri (or stores as a new run result).

    Returns: merged_count, added_count, duplicate_count, output_s3_uri.
    """
    run_id = body.get("run_id", "")
    result_s3_uri = body.get("result_s3_uri", "")
    dedup_key = body.get("dedup_key", "")
    output_s3_uri = body.get("output_s3_uri", "")

    if not run_id:
        return error("run_id is required for merge mode")
    if not result_s3_uri:
        return error("result_s3_uri is required for merge mode")
    if not dedup_key:
        return error("dedup_key is required for merge mode")

    # Load existing dataset
    try:
        existing_rows: list[dict] = _load_s3_uri(result_s3_uri)
    except Exception as e:
        return error(f"Failed to load result_s3_uri {result_s3_uri}: {e}", status_code=404)

    # Load new excavation results
    try:
        new_rows: list[dict] = load_result(run_id)
    except Exception as e:
        return error(f"Failed to load results for {run_id}: {e}", status_code=404)

    # Build key set from existing rows
    existing_keys: set = {
        str(row.get(dedup_key)) for row in existing_rows if dedup_key in row
    }

    added_count = 0
    duplicate_count = 0
    rows_to_add: list[dict] = []
    for row in new_rows:
        key_val = str(row.get(dedup_key, "")) if dedup_key in row else None
        if key_val is None or key_val not in existing_keys:
            rows_to_add.append(row)
            if key_val is not None:
                existing_keys.add(key_val)
            added_count += 1
        else:
            duplicate_count += 1

    merged_rows = existing_rows + rows_to_add
    merged_count = len(merged_rows)

    # Write merged result
    if output_s3_uri:
        # Write to the provided S3 URI
        parts = output_s3_uri.replace("s3://", "").split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else f"merged-{run_id}.json"
        s3_client().put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(merged_rows, default=str),
            ContentType="application/json",
        )
        out_uri = output_s3_uri
    else:
        # Store as a new run result; reuse result_s3_uri path
        parts = result_s3_uri.replace("s3://", "").split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else f"merged-{run_id}.json"
        s3_client().put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(merged_rows, default=str),
            ContentType="application/json",
        )
        out_uri = result_s3_uri

    audit_log("refine", principal, body, {
        "mode": "merge",
        "merged_count": merged_count,
        "added_count": added_count,
        "duplicate_count": duplicate_count,
        "output_s3_uri": out_uri,
    }, request_id=request_id)

    return success({
        "mode": "merge",
        "merged_count": merged_count,
        "added_count": added_count,
        "duplicate_count": duplicate_count,
        "output_s3_uri": out_uri,
    })


def _dedupe(rows: list[dict]) -> list[dict]:
    """Remove duplicate rows based on all fields."""
    seen = set()
    deduped = []
    for row in rows:
        key = json.dumps(row, sort_keys=True)
        if key not in seen:
            seen.add(key)
            deduped.append(row)
    return deduped


def _rank(rows: list[dict], op: str) -> list[dict]:
    """Rank rows by a field. op format: 'rank' or 'rank_by_<field>'."""
    if "_by_" in op:
        field = op.split("_by_", 1)[1]
    else:
        # Default: rank by first numeric field
        field = None
        for row in rows[:1]:
            for k, v in row.items():
                try:
                    float(v)
                    field = k
                    break
                except (ValueError, TypeError):
                    continue

    if field is None:
        return rows

    def sort_key(row: dict) -> float:
        try:
            return -float(row.get(field, 0))
        except (ValueError, TypeError):
            return 0

    return sorted(rows, key=sort_key)


def _filter(rows: list[dict], op_config: dict) -> list[dict]:
    """Filter rows by a field condition.

    op_config keys: field (str), operator (str), value (any)
    Operators: eq, ne, gt, gte, lt, lte, contains, not_contains
    """
    field = op_config.get("field", "")
    operator = op_config.get("operator", "eq")
    value = op_config.get("value")
    if not field:
        return rows

    def _match(row: dict) -> bool:
        if field not in row:
            return True  # field absent → row survives (graceful no-op)
        cell = row.get(field)
        try:
            if operator == "eq":
                return cell == value
            if operator == "ne":
                return cell != value
            if operator == "gt":
                return float(cell) > float(value)  # type: ignore[arg-type]
            if operator == "gte":
                return float(cell) >= float(value)  # type: ignore[arg-type]
            if operator == "lt":
                return float(cell) < float(value)  # type: ignore[arg-type]
            if operator == "lte":
                return float(cell) <= float(value)  # type: ignore[arg-type]
            if operator == "contains":
                return str(value) in str(cell or "")
            if operator == "not_contains":
                return str(value) not in str(cell or "")
        except (TypeError, ValueError):
            return False
        return True

    return [r for r in rows if _match(r)]


def _normalize(rows: list[dict]) -> list[dict]:
    """Normalize field names and types."""
    if not rows:
        return rows

    normalized = []
    for row in rows:
        norm = {}
        for k, v in row.items():
            # Lowercase, underscore field names
            key = k.lower().replace(" ", "_").replace("-", "_")
            # Try to parse numeric strings
            for cast in (int, float):
                try:
                    v = cast(v)
                    break
                except (ValueError, TypeError):
                    continue
            norm[key] = v
        normalized.append(norm)
    return normalized


def _summarize(rows: list[dict], run_id: str, top_k: int) -> dict:
    """Generate an LLM summary of results with grounding check.

    Tries the Quick Suite model router first (if ROUTER_ENDPOINT is set),
    then falls back to direct Bedrock with contextual grounding guardrail.
    """
    data_text = json.dumps(rows[:top_k], indent=2, default=str)
    prompt = (
        f"Summarize the following excavation results concisely.\n"
        f"Focus on key findings, patterns, and notable values.\n\n"
        f"Data ({len(rows)} rows, showing first {min(len(rows), top_k)}):\n"
        f"{data_text}\n\n"
        f"Provide a structured summary with:\n"
        f"1. Key findings (2-3 bullet points)\n"
        f"2. Notable patterns or outliers\n"
        f"3. Data quality observations"
    )

    # Router-first: delegate to Quick Suite model router if configured
    summary_text = call_router("summarize", prompt, max_tokens=1024)

    if summary_text is not None:
        return {
            "type": "summary",
            "text": summary_text,
            "source_run_id": run_id,
            "rows_summarized": min(len(rows), top_k),
        }

    # Fallback: direct Bedrock invocation with guardrail grounding
    invoke_kwargs: dict = {
        "modelId": MODEL_ID,
        "body": json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 1024,
        }),
    }
    if GUARDRAIL_ID:
        invoke_kwargs["guardrailIdentifier"] = GUARDRAIL_ID
        invoke_kwargs["guardrailVersion"] = GUARDRAIL_VERSION

    try:
        response = bedrock_runtime().invoke_model(**invoke_kwargs)
        result = json.loads(response["body"].read())

        summary_text = ""
        for block in result.get("content", []):
            if block.get("type") == "text":
                summary_text += block["text"]

        return {
            "type": "summary",
            "text": summary_text,
            "source_run_id": run_id,
            "rows_summarized": min(len(rows), top_k),
        }

    except Exception as e:
        return {
            "type": "summary",
            "text": f"Summary generation failed: {e}",
            "source_run_id": run_id,
            "error": True,
        }
