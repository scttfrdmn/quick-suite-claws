"""clAWS team_plans tool — list all plans associated with a team_id.

Read-only access: returns plan summaries for a given team_id.
No team-wide execution — callers must use claws.excavate individually.
"""

import json
from typing import Any

from tools.shared import audit_log, error, list_plans_by_team, success


def handler(event: dict, context: Any) -> dict:
    """Lambda handler for claws.team_plans."""
    body = json.loads(event.get("body", "{}")) if isinstance(event.get("body"), str) else event
    team_id = body.get("team_id", "")
    principal = event.get("requestContext", {}).get("authorizer", {}).get("principalId", "unknown")
    request_id = event.get("requestContext", {}).get("requestId", "")

    if not team_id:
        return error("team_id is required")

    plans = list_plans_by_team(team_id)

    summaries = [_summarize(p) for p in plans]
    # Sort newest first
    summaries.sort(key=lambda p: p.get("created_at", ""), reverse=True)

    audit_log(
        "team_plans",
        principal,
        {"team_id": team_id},
        {"count": len(summaries)},
        request_id=request_id,
    )

    return success({"team_id": team_id, "plans": summaries})


def _summarize(plan: dict) -> dict:
    return {
        "plan_id": plan.get("plan_id"),
        "source_id": plan.get("source_id"),
        "query_type": plan.get("query_type"),
        "created_at": plan.get("created_at"),
        "created_by": plan.get("created_by"),
        "team_id": plan.get("team_id"),
    }
