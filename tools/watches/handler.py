"""clAWS watches tool — list active watches and their last-run status."""

import json
from typing import Any

from tools.shared import audit_log, error, list_watches, load_plan, success


def handler(event: dict, context: Any) -> dict:
    """Lambda handler for claws.watches."""
    body = json.loads(event.get("body", "{}")) if isinstance(event.get("body"), str) else event
    status_filter = body.get("status_filter")
    source_id_filter = body.get("source_id_filter")
    team_id_filter = body.get("team_id_filter")
    principal = event.get("requestContext", {}).get("authorizer", {}).get("principalId", "unknown")
    request_id = event.get("requestContext", {}).get("requestId", "")

    if status_filter and status_filter not in ("active", "paused", "errored"):
        return error("status_filter must be active, paused, or errored")

    watches = list_watches(status_filter=status_filter, team_id_filter=team_id_filter)

    # Apply source_id_filter — source_id is denormalized onto the watch record at creation
    if source_id_filter:
        watches = [w for w in watches if w.get("source_id") == source_id_filter]

    result = [_format_watch(w) for w in watches]

    audit_log("watches", principal, {
        "status_filter": status_filter,
        "source_id_filter": source_id_filter,
        "team_id_filter": team_id_filter,
    }, {"count": len(result)}, request_id=request_id)

    return success({"watches": result})


def _format_watch(w: dict) -> dict:
    return {
        "watch_id": w.get("watch_id"),
        "plan_id": w.get("plan_id"),
        "source_id": w.get("source_id"),
        "schedule": w.get("schedule"),
        "condition": w.get("condition"),
        "status": w.get("status", "active"),
        "last_run_at": w.get("last_run_at"),
        "last_run_id": w.get("last_run_id"),
        "last_triggered_at": w.get("last_triggered_at"),
    }
