"""clAWS refine tool — post-process excavation results.

Operations: dedupe, rank, filter, summarize, normalize.
The summarize operation uses Bedrock with guardrail for
contextual grounding checks.
"""

import json
import os
from typing import Any

from tools.shared import (
    GUARDRAIL_ID,
    GUARDRAIL_VERSION,
    audit_log,
    bedrock_runtime,
    error,
    load_result,
    new_run_id,
    scan_payload,
    store_result,
    success,
)

MODEL_ID = os.environ.get("CLAWS_REFINE_MODEL_ID", "anthropic.claude-sonnet-4-20250514-v1:0")

ALLOWED_OPERATIONS = {"dedupe", "rank", "rank_by_n", "filter", "summarize", "normalize"}


def handler(event: dict, context: Any) -> dict:
    """Lambda handler for claws.refine."""
    body = json.loads(event.get("body", "{}")) if isinstance(event.get("body"), str) else event
    run_id = body.get("run_id", "")
    operations = body.get("operations", [])
    top_k = body.get("top_k", 25)
    principal = event.get("requestContext", {}).get("authorizer", {}).get("principalId", "unknown")

    if not run_id:
        return error("run_id is required")
    if not operations:
        return error("operations is required")

    # Validate operations — rank_by_<field> is a valid rank variant
    invalid = [
        op for op in operations
        if op not in ALLOWED_OPERATIONS and not op.startswith("rank_by_")
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

        if op == "dedupe":
            rows = _dedupe(rows)
        elif op.startswith("rank"):
            rows = _rank(rows, op)
        elif op == "filter":
            rows = rows  # TODO: parameterized filtering
        elif op == "normalize":
            rows = _normalize(rows)
        elif op == "summarize":
            rows = _summarize(rows, run_id, top_k)

        after_count = len(rows) if isinstance(rows, list) else 1
        manifest["operations"].append({
            "operation": op,
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
    })

    return success({
        "run_id": refined_run_id,
        "refined_uri": result_uri,
        "manifest": manifest,
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

    Uses Bedrock Guardrails contextual grounding to verify the
    summary is faithful to the source data.
    """
    data_text = json.dumps(rows[:top_k], indent=2, default=str)

    invoke_kwargs = {
        "modelId": MODEL_ID,
        "body": json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "messages": [{
                "role": "user",
                "content": f"""Summarize the following excavation results concisely.
Focus on key findings, patterns, and notable values.

Data ({len(rows)} rows, showing first {min(len(rows), top_k)}):
{data_text}

Provide a structured summary with:
1. Key findings (2-3 bullet points)
2. Notable patterns or outliers
3. Data quality observations""",
            }],
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
