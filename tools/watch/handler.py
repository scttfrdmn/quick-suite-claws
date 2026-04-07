"""clAWS watch tool — create, update, and delete scheduled watches.

A watch locks a plan at creation time. The watch runner executes the stored
plan on the configured schedule, evaluates an optional condition, and fires
a notification target if triggered. No LLM at execution time.
"""

import json
import os
import re
import time
from datetime import UTC, datetime
from typing import Any

import boto3

from tools.errors import NotFoundError
from tools.shared import (
    audit_log,
    delete_watch,
    error,
    load_plan,
    load_watch,
    new_watch_id,
    store_watch,
    success,
    update_watch,
)

# Valid action_routing destination types
_ACTION_ROUTING_TYPES = frozenset({"sns", "eventbridge", "bedrock_agent"})

# EventBridge Scheduler group all watch schedules live in
SCHEDULE_GROUP = "claws-watches"
WATCH_RUNNER_ARN = os.environ.get("CLAWS_WATCH_RUNNER_ARN", "")
WATCH_RUNNER_ROLE_ARN = os.environ.get("CLAWS_WATCH_RUNNER_ROLE_ARN", "")

_scheduler = None


def scheduler_client() -> Any:
    global _scheduler
    if _scheduler is None:
        _scheduler = boto3.client("scheduler")
    return _scheduler


_SCHEDULE_RE = re.compile(r"^(rate\(.+\)|cron\(.+\))$")


def _validate_schedule(schedule: str) -> bool:
    return bool(_SCHEDULE_RE.match(schedule.strip()))


def _create_schedule(watch_id: str, schedule: str, target_input: dict) -> None:
    """Create an EventBridge Scheduler schedule for a watch."""
    if not WATCH_RUNNER_ARN or not WATCH_RUNNER_ROLE_ARN:
        return  # Not configured — skip (unit test / dev environment)
    scheduler_client().create_schedule(
        Name=f"claws-watch-{watch_id}",
        GroupName=SCHEDULE_GROUP,
        ScheduleExpression=schedule,
        FlexibleTimeWindow={"Mode": "OFF"},
        Target={
            "Arn": WATCH_RUNNER_ARN,
            "RoleArn": WATCH_RUNNER_ROLE_ARN,
            "Input": json.dumps(target_input),
        },
    )


def _update_schedule(watch_id: str, schedule: str, target_input: dict) -> None:
    if not WATCH_RUNNER_ARN or not WATCH_RUNNER_ROLE_ARN:
        return
    scheduler_client().update_schedule(
        Name=f"claws-watch-{watch_id}",
        GroupName=SCHEDULE_GROUP,
        ScheduleExpression=schedule,
        FlexibleTimeWindow={"Mode": "OFF"},
        Target={
            "Arn": WATCH_RUNNER_ARN,
            "RoleArn": WATCH_RUNNER_ROLE_ARN,
            "Input": json.dumps(target_input),
        },
    )


def _delete_schedule(watch_id: str) -> None:
    if not WATCH_RUNNER_ARN:
        return
    try:
        scheduler_client().delete_schedule(
            Name=f"claws-watch-{watch_id}",
            GroupName=SCHEDULE_GROUP,
        )
    except Exception:
        pass  # Already deleted or never created — safe to ignore


def handler(event: dict, context: Any) -> dict:
    """Lambda handler for claws.watch."""
    body = json.loads(event.get("body", "{}")) if isinstance(event.get("body"), str) else event
    action = body.get("action", "")
    principal = event.get("requestContext", {}).get("authorizer", {}).get("principalId", "unknown")
    request_id = event.get("requestContext", {}).get("requestId", "")

    if action not in ("create", "update", "delete"):
        return error("action must be create, update, or delete")

    if action == "create":
        return _create(body, principal, request_id)
    if action == "update":
        return _update(body, principal, request_id)
    return _delete(body, principal, request_id)


def _create(body: dict, principal: str, request_id: str) -> dict:
    plan_id = body.get("plan_id", "")
    schedule = body.get("schedule", "")
    condition = body.get("condition")
    notification_target = body.get("notification_target", {})
    ttl_days = int(body.get("ttl_days", 90))

    if not plan_id:
        return error("plan_id is required for create")
    if not schedule:
        return error("schedule is required for create")
    if not _validate_schedule(schedule):
        return error("schedule must be a valid rate() or cron() expression")

    # Validate plan exists (422 = unprocessable — referenced plan must exist)
    plan = load_plan(plan_id)
    if plan is None:
        return error(f"Plan {plan_id} not found", status_code=422)

    watch_id = new_watch_id()
    now = datetime.now(UTC).isoformat()

    # Validate watch type
    watch_type = body.get("type", "alert")
    if watch_type not in ("alert", "feed", "new_award"):
        return error(f"type must be 'alert', 'feed', or 'new_award', got: {watch_type!r}")

    # feed watches require a dedup_key
    feed_dedup_key = body.get("feed_dedup_key", "")
    if watch_type == "feed" and not feed_dedup_key:
        return error("feed_dedup_key is required for feed watches")

    # new_award watches require semantic_match with lab_profile_ssm_key
    semantic_match = body.get("semantic_match")
    if watch_type == "new_award":
        if not semantic_match or not isinstance(semantic_match, dict):
            return error("semantic_match is required for new_award watches")
        if not semantic_match.get("lab_profile_ssm_key"):
            return error("semantic_match.lab_profile_ssm_key is required for new_award watches")

    # compliance watches require a ruleset URI
    compliance_mode = bool(body.get("compliance_mode"))
    compliance_ruleset_uri = body.get("compliance_ruleset_uri", "")
    if compliance_mode and not compliance_ruleset_uri:
        return error("compliance_ruleset_uri is required when compliance_mode is true")

    # action_routing: validate destination_type if provided
    action_routing = body.get("action_routing")
    if action_routing is not None:
        if not isinstance(action_routing, dict):
            return error("action_routing must be an object")
        ar_type = action_routing.get("destination_type", "")
        if ar_type not in _ACTION_ROUTING_TYPES:
            return error(
                f"action_routing.destination_type must be one of: {sorted(_ACTION_ROUTING_TYPES)}"
            )
        if not action_routing.get("destination_arn"):
            return error("action_routing.destination_arn is required")

    # accreditation_config_uri: optional, no deep validation at create time
    accreditation_config_uri = body.get("accreditation_config_uri", "")

    spec = {
        "plan_id": plan_id,
        "source_id": plan.get("source_id", ""),   # denormalized for watches listing
        "schedule": schedule,
        "type": watch_type,
        "status": "active",
        "notification_target": notification_target,
        "ttl": int(time.time()) + ttl_days * 86400,
        "created_at": now,
        "last_run_id": None,
        "last_run_at": None,
        "last_triggered_at": None,
        "consecutive_errors": 0,
    }
    if condition:
        spec["condition"] = condition
    if watch_type == "feed":
        spec["feed_dedup_key"] = feed_dedup_key
        spec["feed_result_uri"] = None  # filled by runner after first run
    if watch_type == "new_award" and semantic_match:
        spec["semantic_match"] = semantic_match
    if compliance_mode:
        spec["compliance_mode"] = True
        spec["compliance_ruleset_uri"] = compliance_ruleset_uri
    if accreditation_config_uri:
        spec["accreditation_config_uri"] = accreditation_config_uri
    if action_routing:
        spec["action_routing"] = action_routing
    # Denormalize team_id from plan at watch creation so watches can be filtered by team
    if plan.get("team_id"):
        spec["team_id"] = plan["team_id"]

    store_watch(watch_id, spec)
    _create_schedule(watch_id, schedule, {"watch_id": watch_id})

    audit_log("watch", principal, {"action": "create", "plan_id": plan_id},
              {"status": "created", "watch_id": watch_id}, request_id=request_id)

    return success({"watch_id": watch_id, "status": "created"})


def _update(body: dict, principal: str, request_id: str) -> dict:
    watch_id = body.get("watch_id", "")
    if not watch_id:
        return error("watch_id is required for update")

    watch = load_watch(watch_id)
    if watch is None:
        return error(NotFoundError(f"Watch {watch_id} not found"))

    updates: dict = {}
    if "schedule" in body:
        if not _validate_schedule(body["schedule"]):
            return error("schedule must be a valid rate() or cron() expression")
        updates["schedule"] = body["schedule"]
    if "condition" in body:
        updates["condition"] = body["condition"]
    if "notification_target" in body:
        updates["notification_target"] = body["notification_target"]
    if "status" in body:
        updates["status"] = body["status"]
    if "action_routing" in body:
        updates["action_routing"] = body["action_routing"]
    if "accreditation_config_uri" in body:
        updates["accreditation_config_uri"] = body["accreditation_config_uri"]

    if updates:
        update_watch(watch_id, updates)

    new_schedule = updates.get("schedule", watch.get("schedule", ""))
    _update_schedule(watch_id, new_schedule, {"watch_id": watch_id})

    audit_log("watch", principal, {"action": "update", "watch_id": watch_id},
              {"status": "updated", "watch_id": watch_id}, request_id=request_id)

    return success({"watch_id": watch_id, "status": "updated"})


def _delete(body: dict, principal: str, request_id: str) -> dict:
    watch_id = body.get("watch_id", "")
    if not watch_id:
        return error("watch_id is required for delete")

    watch = load_watch(watch_id)
    if watch is None:
        return error(NotFoundError(f"Watch {watch_id} not found"))

    delete_watch(watch_id)
    _delete_schedule(watch_id)

    audit_log("watch", principal, {"action": "delete", "watch_id": watch_id},
              {"status": "deleted", "watch_id": watch_id}, request_id=request_id)

    return success({"watch_id": watch_id, "status": "deleted"})
