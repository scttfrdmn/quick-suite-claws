"""clAWS instantiate_plan tool — create a concrete plan from a template (#66).

Takes a template plan (created with is_template=True via claws.plan) and a dict
of values to substitute into the {{variable}} placeholders in the objective.
Returns a new concrete plan_id, identical to calling claws.plan directly.
"""

import json
import re
from typing import Any

from tools.shared import (
    audit_log,
    error,
    load_plan,
)

# Pattern matching {{variable_name}} placeholders
_TEMPLATE_VAR_RE = re.compile(r"\{\{[^{}]+\}\}")


def _resolve_template(objective: str, values: dict) -> tuple[str, str | None]:
    """Substitute {{var}} placeholders in objective with values from dict.

    Returns (resolved_objective, error_message).
    error_message is None on success.
    """
    for key, val in values.items():
        # Guard against values that themselves contain {{ to prevent injection
        str_val = str(val)
        if "{{" in str_val:
            return "", f"Template value for '{key}' contains '{{{{' — nested templates are not allowed"
        objective = objective.replace(f"{{{{{key}}}}}", str_val)

    # Check for any remaining unresolved placeholders
    remaining = _TEMPLATE_VAR_RE.findall(objective)
    if remaining:
        missing = [r[2:-2] for r in remaining]  # strip {{ and }}
        return "", f"Missing template values for: {', '.join(missing)}"

    return objective, None


def handler(event: dict, context: Any) -> dict:
    """Lambda handler for claws.instantiate_plan."""
    body = json.loads(event.get("body", "{}")) if isinstance(event.get("body"), str) else event
    plan_id = body.get("plan_id", "")
    values: dict = body.get("values", {})
    authorizer = event.get("requestContext", {}).get("authorizer", {})
    principal = authorizer.get("principalId", "unknown")
    request_id = event.get("requestContext", {}).get("requestId", "")

    if not plan_id:
        return error("plan_id is required")

    # Load and validate the template plan
    template = load_plan(plan_id)
    if template is None:
        return error(f"Plan {plan_id} not found", status_code=404)

    if template.get("status") != "template":
        return error(
            f"Plan {plan_id} is not a template (status: '{template.get('status', 'unknown')}'). "
            "Only plans created with is_template=True can be instantiated.",
            status_code=422,
        )

    objective_template = template.get("objective", "")
    if not objective_template:
        return error("Template plan has no objective stored", status_code=422)

    # Resolve template variables
    resolved_objective, resolve_error = _resolve_template(objective_template, values)
    if resolve_error:
        return error(resolve_error)

    audit_log("instantiate_plan", principal, {
        "plan_id": plan_id,
        "values_count": len(values),
    }, {"status": "resolving"}, request_id=request_id)

    # Delegate to the plan handler with the resolved objective.
    # Build a synthetic event that looks like a claws.plan invocation.
    from tools.plan.handler import handler as plan_handler  # noqa: PLC0415

    plan_event = {
        "objective": resolved_objective,
        "source_id": template.get("source_id", ""),
        "constraints": template.get("constraints", {}),
        "requires_irb": template.get("requires_irb", False),
    }
    if template.get("team_id"):
        plan_event["team_id"] = template["team_id"]

    # Forward authorizer context so plan handler gets correct principal + roles
    forwarded_event = {
        **plan_event,
        "requestContext": event.get("requestContext", {
            "authorizer": {"principalId": principal, "roles": "[]"},
            "requestId": request_id,
        }),
    }

    result = plan_handler(forwarded_event, context)

    # Enrich the response with template provenance
    if isinstance(result, dict):
        body_out = result.get("body")
        if isinstance(body_out, str):
            try:
                parsed = json.loads(body_out)
                parsed["instantiated_from"] = plan_id
                result = {**result, "body": json.dumps(parsed)}
            except (json.JSONDecodeError, TypeError):
                pass
        elif isinstance(result.get("plan_id"), str):
            # Direct dict response (non-API-GW invocation path)
            result = {**result, "instantiated_from": plan_id}

    return result
