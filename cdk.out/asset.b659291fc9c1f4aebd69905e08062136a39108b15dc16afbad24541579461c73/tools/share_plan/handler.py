"""clAWS share_plan tool — grant read/excavate access on a plan to other principals.

Only the plan owner (created_by) can call share_plan on their own plans.
Writes a shared_with list onto the plan item. The excavate tool checks this
list when the principal is not the original owner.

Cedar action: plan.share (only permitted on own plans).
"""

import json
from typing import Any

from tools.errors import ForbiddenError, NotFoundError
from tools.shared import audit_log, error, load_plan, share_plan, success


def handler(event: dict, context: Any) -> dict:
    """Lambda handler for claws.share_plan."""
    body = json.loads(event.get("body", "{}")) if isinstance(event.get("body"), str) else event
    plan_id = body.get("plan_id", "")
    share_with = body.get("share_with", [])  # list of principal IDs
    principal = event.get("requestContext", {}).get("authorizer", {}).get("principalId", "unknown")
    request_id = event.get("requestContext", {}).get("requestId", "")

    if not plan_id:
        return error("plan_id is required")
    if not isinstance(share_with, list):
        return error("share_with must be a list of principal IDs")

    plan = load_plan(plan_id)
    if plan is None:
        return error(NotFoundError(f"Plan {plan_id} not found"))

    # Only the plan owner may share the plan
    plan_owner = plan.get("created_by", "")
    if plan_owner and principal != plan_owner:
        audit_log("share_plan", principal, {"plan_id": plan_id}, {
            "status": "rejected",
            "reason": "Only the plan owner can share this plan",
        }, request_id=request_id)
        return error(ForbiddenError("Only the plan owner can share this plan"))

    ok = share_plan(plan_id, share_with)
    if not ok:
        return error(NotFoundError(f"Plan {plan_id} not found"))

    audit_log("share_plan", principal, {"plan_id": plan_id, "share_with": share_with}, {
        "status": "shared",
        "shared_count": len(share_with),
    }, request_id=request_id)

    return success({
        "plan_id": plan_id,
        "status": "shared",
        "shared_with": share_with,
    })
