"""clAWS approve_plan — internal Lambda for IRB plan approval.

NOT an AgentCore tool. Invoked by IRB reviewers via direct Lambda invocation
or an internal API. Approves plans that have status "pending_approval".

Cedar action: plan.approve — only principals with irb_approver role may call this.
Structural rule enforced here: an approver cannot approve their own plan.
"""

import json
import os
from datetime import UTC, datetime
from typing import Any

import boto3

from tools.errors import ForbiddenError, NotFoundError, ValidationError
from tools.shared import audit_log, dynamodb_resource, error, load_plan, success

PLANS_TABLE = os.environ.get("CLAWS_PLANS_TABLE", "claws-plans")

# Principals with the irb_approver role are listed in the
# CLAWS_IRB_APPROVERS environment variable as a comma-separated list.
# In production this would be replaced by a Cedar policy evaluation call
# via AgentCore, but for the Lambda boundary we check the env var.
IRB_APPROVERS_ENV = os.environ.get("CLAWS_IRB_APPROVERS", "")

# EventBridge bus name for approval events
EVENTS_BUS = os.environ.get("CLAWS_EVENTS_BUS", "default")


def _get_irb_approvers() -> set[str]:
    """Return the set of authorised IRB approver principal IDs."""
    raw = os.environ.get("CLAWS_IRB_APPROVERS", "")
    return {p.strip() for p in raw.split(",") if p.strip()}


def handler(event: dict, context: Any) -> dict:
    """Lambda handler for clAWS internal approve_plan action.

    Input keys:
      - plan_id        (required) — plan to approve
      - approved_by    (required) — principal ID of the reviewer
      - approval_notes (optional) — free-text notes from the reviewer
    """
    body = json.loads(event.get("body", "{}")) if isinstance(event.get("body"), str) else event
    plan_id = body.get("plan_id", "")
    approved_by = body.get("approved_by", "")
    approval_notes = body.get("approval_notes")

    if not plan_id:
        return error(ValidationError("plan_id is required"))
    if not approved_by:
        return error(ValidationError("approved_by is required"))

    # Check that the approver has the irb_approver role.
    # In production this is enforced by Cedar plan.approve action at the Gateway.
    # For the Lambda boundary we check the CLAWS_IRB_APPROVERS allowlist.
    approvers = _get_irb_approvers()
    if approvers and approved_by not in approvers:
        audit_log(
            "approve_plan",
            approved_by,
            {"plan_id": plan_id},
            {"status": "rejected", "reason": "Principal is not an authorised IRB approver"},
        )
        return error(ForbiddenError(
            "Not authorized to approve plans. "
            "Principal does not have the irb_approver role."
        ))

    plan = load_plan(plan_id)
    if plan is None:
        return error(NotFoundError(f"Plan {plan_id} not found"))

    # Only plans explicitly marked requires_irb can be approved through this pathway.
    if not plan.get("requires_irb"):
        return error(ValidationError(
            "Plan does not require IRB approval — cannot approve"
        ))

    # A principal cannot approve their own plan (conflict of interest).
    plan_owner = plan.get("created_by", "")
    if plan_owner and approved_by == plan_owner:
        audit_log(
            "approve_plan",
            approved_by,
            {"plan_id": plan_id},
            {"status": "rejected", "reason": "Approver cannot approve their own plan"},
        )
        return error(ForbiddenError("Approver cannot approve their own plan"))

    current_status = plan.get("status", "ready")
    if current_status not in ("pending_approval", "approved"):
        # Only pending or already-approved plans are valid targets.
        return error(ValidationError(
            f"Plan {plan_id} has status '{current_status}' and cannot be approved"
        ))

    # Set the plan status to approved
    approved_at = datetime.now(UTC).isoformat()
    table = dynamodb_resource().Table(PLANS_TABLE)
    update_expr = "SET #s = :s, approved_by = :ab, approved_at = :aa"
    expr_names = {"#s": "status"}
    expr_values: dict = {
        ":s": "approved",
        ":ab": approved_by,
        ":aa": approved_at,
    }
    if approval_notes:
        update_expr += ", approval_notes = :an"
        expr_values[":an"] = approval_notes

    table.update_item(
        Key={"plan_id": plan_id},
        UpdateExpression=update_expr,
        ExpressionAttributeNames=expr_names,
        ExpressionAttributeValues=expr_values,
    )

    # Emit EventBridge event
    _emit_approval_event(plan_id, approved_by, approved_at, approval_notes)

    audit_log(
        "approve_plan",
        approved_by,
        {"plan_id": plan_id},
        {"status": "approved", "approved_at": approved_at},
    )

    return success({
        "plan_id": plan_id,
        "status": "approved",
        "approved_by": approved_by,
        "approved_at": approved_at,
        **({"approval_notes": approval_notes} if approval_notes else {}),
    })


def _emit_approval_event(
    plan_id: str,
    approved_by: str,
    approved_at: str,
    approval_notes: str | None,
) -> None:
    """Put a PlanApproved event onto EventBridge. Failures are swallowed."""
    try:
        events = boto3.client("events")
        detail: dict = {
            "plan_id": plan_id,
            "approved_by": approved_by,
            "approved_at": approved_at,
        }
        if approval_notes:
            detail["approval_notes"] = approval_notes
        events.put_events(Entries=[{
            "Source": "claws.irb",
            "DetailType": "PlanApproved",
            "Detail": json.dumps(detail),
            "EventBusName": EVENTS_BUS,
        }])
    except Exception as exc:
        print(json.dumps({
            "level": "warn",
            "msg": "approve_plan: failed to emit EventBridge event",
            "error": str(exc),
        }))
