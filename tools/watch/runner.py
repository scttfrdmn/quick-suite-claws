"""clAWS watch runner — invoked by EventBridge Scheduler.

Not an AgentCore tool. Receives {"watch_id": "watch-..."} from the scheduler,
executes the locked plan, evaluates the optional condition, and fires the
notification target if triggered. No LLM is invoked at execution time.
"""

import json
import os
from datetime import UTC, datetime
from typing import Any

from tools.excavate.handler import EXECUTORS
from tools.shared import (
    RUNS_BUCKET,
    audit_log,
    load_plan,
    load_watch,
    new_run_id,
    store_result,
    store_result_metadata,
    update_watch,
)
from tools.excavate.handler import _infer_schema

# Maximum consecutive executor errors before a watch is paused
MAX_CONSECUTIVE_ERRORS = int(os.environ.get("CLAWS_WATCH_MAX_ERRORS", "3"))


def handler(event: dict, context: Any) -> dict:
    """Lambda handler for the watch runner.

    EventBridge Scheduler passes {"watch_id": "watch-..."} as the schedule input.
    """
    watch_id = event.get("watch_id", "")
    if not watch_id:
        return {"status": "error", "error": "watch_id missing from event"}

    watch = load_watch(watch_id)
    if watch is None:
        return {"status": "error", "error": f"Watch {watch_id} not found"}

    if watch.get("status") in ("paused", "deleted"):
        return {"status": "skipped", "reason": watch.get("status")}

    plan = load_plan(watch["plan_id"])
    if plan is None:
        _mark_errored(watch_id, watch, "Plan not found")
        return {"status": "error", "error": "Plan not found"}

    executor = EXECUTORS.get(plan.get("query_type", ""))
    if executor is None:
        _mark_errored(watch_id, watch, f"Unsupported query_type: {plan.get('query_type')}")
        return {"status": "error", "error": "Unsupported query_type"}

    run_id = new_run_id()
    now = datetime.now(UTC).isoformat()

    try:
        exec_result = executor(
            source_id=plan["source_id"],
            query=plan["query"],
            constraints=plan.get("constraints", {}),
            run_id=run_id,
        )
    except Exception as exc:
        _mark_errored(watch_id, watch, str(exc))
        return {"status": "error", "error": str(exc)}

    if exec_result.get("status") == "error":
        _mark_errored(watch_id, watch, exec_result.get("error", "executor error"))
        return {"status": "error", "error": exec_result.get("error")}

    rows = exec_result.get("rows", [])
    store_result(run_id, rows)
    store_result_metadata(
        run_id=run_id,
        schema=_infer_schema(rows),
        row_count=len(rows),
        bytes_scanned=exec_result.get("bytes_scanned", 0),
        cost=exec_result.get("cost", "$0.00"),
        source_id=plan["source_id"],
    )

    triggered = _evaluate_condition(watch.get("condition"), rows)
    triggered_at = now if triggered else watch.get("last_triggered_at")

    if triggered and watch.get("notification_target"):
        _fire_notification(watch["notification_target"], run_id, rows, watch_id)

    update_watch(watch_id, {
        "last_run_id": run_id,
        "last_run_at": now,
        "last_triggered_at": triggered_at,
        "consecutive_errors": 0,
        "status": "active",
    })

    audit_log(
        "watch-runner",
        "watch-scheduler",
        {"watch_id": watch_id},
        {
            "status": "complete",
            "watch_id": watch_id,
            "run_id": run_id,
            "rows_returned": len(rows),
            "triggered": triggered,
        },
    )

    return {"status": "complete", "watch_id": watch_id, "run_id": run_id, "triggered": triggered}


def _evaluate_condition(condition: dict | None, rows: list[dict]) -> bool:
    """Return True if the condition is satisfied (or absent — always fires)."""
    if not condition:
        return True

    field = condition.get("field", "")
    operator = condition.get("operator", "")
    threshold = condition.get("threshold")

    if not field or not operator or threshold is None:
        return True

    # Collect values for the field across all rows
    values = [row[field] for row in rows if field in row]
    if not values:
        return False

    # Use max for ordered comparisons; check any for eq/ne
    try:
        if operator == "gt":
            return max(values) > threshold
        if operator == "gte":
            return max(values) >= threshold
        if operator == "lt":
            return min(values) < threshold
        if operator == "lte":
            return min(values) <= threshold
        if operator == "eq":
            return any(v == threshold for v in values)
        if operator == "ne":
            return any(v != threshold for v in values)
    except (TypeError, ValueError):
        pass
    return False


def _fire_notification(target: dict, run_id: str, rows: list[dict], watch_id: str) -> None:
    """Dispatch to the export handler's notification logic."""
    target_type = target.get("type", "")
    uri = target.get("uri", "")
    if not target_type or not uri:
        return

    try:
        if target_type == "s3":
            import boto3
            import json as _json
            s3 = boto3.client("s3")
            parts = uri.replace("s3://", "").split("/", 1)
            bucket, key = parts[0], parts[1] if len(parts) > 1 else f"watch-{watch_id}/{run_id}.json"
            s3.put_object(Bucket=bucket, Key=key, Body=_json.dumps(rows, default=str),
                          ContentType="application/json")
        elif target_type == "eventbridge":
            import boto3
            events = boto3.client("events")
            bus_parts = uri.replace("events://", "").split("/", 1)
            bus_name = bus_parts[0]
            detail_type = bus_parts[1] if len(bus_parts) > 1 else "ClawsWatchTriggered"
            events.put_events(Entries=[{
                "EventBusName": bus_name,
                "Source": "claws.watch",
                "DetailType": detail_type,
                "Detail": json.dumps({"watch_id": watch_id, "run_id": run_id,
                                      "rows_returned": len(rows)}),
            }])
    except Exception as exc:
        print(json.dumps({"level": "warn", "msg": "fire_notification failed",
                          "error": str(exc), "watch_id": watch_id}))


def _mark_errored(watch_id: str, watch: dict, error_detail: str) -> None:
    consecutive = int(watch.get("consecutive_errors", 0)) + 1
    new_status = "paused" if consecutive >= MAX_CONSECUTIVE_ERRORS else "errored"
    update_watch(watch_id, {
        "status": new_status,
        "error_detail": error_detail,
        "consecutive_errors": consecutive,
        "last_run_at": datetime.now(UTC).isoformat(),
    })
