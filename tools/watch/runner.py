"""clAWS watch runner — invoked by EventBridge Scheduler.

Not an AgentCore tool. Receives {"watch_id": "watch-..."} from the scheduler,
executes the locked plan, evaluates the optional condition, and fires the
notification target if triggered. No LLM is invoked at execution time.

Feed watches (type="feed") accumulate results across runs by calling refine with
mode="merge" after each execution. The merged dataset URI is persisted in
feed_result_uri on the watch spec and passed as result_s3_uri to the next run.
"""

import json
import logging
import os
from datetime import UTC, datetime
from typing import Any

import boto3

from tools.excavate.handler import EXECUTORS, _infer_schema
from tools.shared import (
    RUNS_BUCKET,
    audit_log,
    call_router,
    load_config_from_uri,
    load_plan,
    load_watch,
    new_run_id,
    store_result,
    store_result_metadata,
    update_watch,
)

# Maximum consecutive executor errors before a watch is paused
MAX_CONSECUTIVE_ERRORS = int(os.environ.get("CLAWS_WATCH_MAX_ERRORS", "3"))

# Cap on awards scored per new_award watch run (prevents runaway Router spend)
_NEW_AWARD_MAX_ROWS = 50

_ssm_client: Any = None


def _ssm() -> Any:
    global _ssm_client
    if _ssm_client is None:
        _ssm_client = boto3.client("ssm")
    return _ssm_client


def _run_new_award_semantic_match(rows: list, cfg: dict) -> list:
    """Score each award row for semantic similarity to a lab profile via the Router.

    Fetches the lab abstract from SSM, calls Router `summarize` per award (up to
    _NEW_AWARD_MAX_ROWS rows), and returns rows whose similarity score meets or
    exceeds the configured threshold. Router failures are non-blocking — a warning
    is logged and the award is skipped.

    Args:
        rows: Award rows from the excavation result.
        cfg: semantic_match config dict from the watch spec. Required key:
             "lab_profile_ssm_key" (SSM parameter path). Optional:
             "abstract_similarity_threshold" (float, default 0.82).
             "abstract_field" (str, default "abstract_text" or "AbstractText").

    Returns:
        List of matching rows sorted by similarity descending.
    """
    ssm_key = cfg.get("lab_profile_ssm_key", "")
    threshold = float(cfg.get("abstract_similarity_threshold", 0.82))
    abstract_field = cfg.get("abstract_field", "")

    # Fetch lab profile from SSM
    try:
        param = _ssm().get_parameter(Name=ssm_key)
        lab_abstract = param["Parameter"]["Value"]
    except Exception as exc:
        logging.warning(json.dumps({
            "msg": "new_award: lab profile SSM fetch failed",
            "ssm_key": ssm_key,
            "error": str(exc),
        }))
        return []

    matches = []
    for row in rows[:_NEW_AWARD_MAX_ROWS]:
        # Find abstract field: try explicit cfg, then common names
        if abstract_field:
            award_abstract = str(row.get(abstract_field, ""))
        else:
            award_abstract = str(
                row.get("abstract_text")
                or row.get("AbstractText")
                or row.get("abstract")
                or ""
            )

        prompt = (
            f"Rate the semantic similarity between the following two texts on a scale "
            f"of 0.0 to 1.0, where 1.0 is identical in meaning. "
            f"Reply with ONLY the numeric score, nothing else.\n\n"
            f"LAB PROFILE:\n{lab_abstract[:2000]}\n\n"
            f"AWARD ABSTRACT:\n{award_abstract[:2000]}"
        )
        try:
            response_text = call_router("summarize", prompt, max_tokens=10)
            if response_text is None:
                raise ValueError("Router not configured or returned None")
            score = float(response_text.strip().split()[0])
        except Exception as exc:
            logging.warning(json.dumps({
                "msg": "new_award: Router similarity scoring failed; skipping award",
                "error": str(exc),
            }))
            continue

        if score >= threshold:
            matches.append({**row, "_similarity_score": score})

    matches.sort(key=lambda r: r.get("_similarity_score", 0.0), reverse=True)
    return matches


def _run_action_routing(
    watch: dict,
    triggered_rows: list,
    diff_summary: dict | None,
    run_id: str,
) -> None:
    """Draft a context-specific response via Router and dispatch to the configured destination.

    Template substitution: ``{key}`` placeholders in `context_template` are replaced
    using the `diff_summary` dict (for drift-based watches).  Substitution failures
    leave the placeholder literal.

    Delivery:
    - ``sns``        → ``boto3.client("sns").publish(...)``
    - ``eventbridge`` → ``boto3.client("events").put_events(...)``
    - ``bedrock_agent`` → not yet implemented; logs WARNING and returns

    Fail-open: if the Router call fails, ``draft_text`` is ``None`` and the raw
    payload is still delivered.
    """
    ar = watch.get("action_routing", {})
    dest_type = ar.get("destination_type", "")
    dest_arn = ar.get("destination_arn", "")
    template = ar.get("context_template", "")
    watch_id = watch.get("watch_id", "")

    if not dest_type or not dest_arn:
        return

    # Substitute {key} placeholders from diff_summary
    filled = template
    if diff_summary:
        for k, v in diff_summary.items():
            filled = filled.replace(f"{{{k}}}", str(v))

    # Draft response via Router (fail-open)
    draft_text: str | None = None
    if filled:
        try:
            draft_text = call_router("summarize", filled, max_tokens=500)
        except Exception as exc:
            logging.warning(json.dumps({
                "msg": "action_routing: Router call failed; delivering without draft",
                "watch_id": watch_id,
                "error": str(exc),
            }))

    payload = {
        "watch_id": watch_id,
        "run_id": run_id,
        "triggered_rows": triggered_rows,
        "draft_text": draft_text,
        "diff_summary": diff_summary,
        "timestamp": datetime.now(UTC).isoformat(),
    }

    try:
        if dest_type == "sns":
            boto3.client("sns").publish(
                TopicArn=dest_arn,
                Message=json.dumps(payload, default=str),
            )
        elif dest_type == "eventbridge":
            bus_parts = dest_arn.replace("events://", "").split("/", 1)
            bus_name = bus_parts[0] if bus_parts else dest_arn
            detail_type = bus_parts[1] if len(bus_parts) > 1 else "ClawsWatchActionRouting"
            boto3.client("events").put_events(Entries=[{
                "EventBusName": bus_name,
                "Source": "claws.watch",
                "DetailType": detail_type,
                "Detail": json.dumps(payload, default=str),
            }])
        elif dest_type == "bedrock_agent":
            logging.warning(json.dumps({
                "msg": "action_routing: bedrock_agent destination not yet supported",
                "watch_id": watch_id,
            }))
    except Exception as exc:
        logging.warning(json.dumps({
            "msg": "action_routing: delivery failed",
            "dest_type": dest_type,
            "watch_id": watch_id,
            "error": str(exc),
        }))


def _evaluate_accreditation(watch: dict, rows: list) -> list:
    """Evaluate accreditation evidence predicates against excavation results.

    Loads the AccreditationConfig from ``watch["accreditation_config_uri"]``
    (S3 or SSM URI). For each standard, evaluates its ``evidence_predicate``
    against the result rows using ``_evaluate_condition()``.  Standards with
    no satisfying evidence are returned as gap dicts.

    Returns a list of gap dicts (empty if all standards are satisfied).
    Each gap: ``{standard_id, description, evidence_predicate}``.

    Config format::

        {
          "standards": {
            "SACSCOC-8.2.c": {
              "description": "Faculty credential verification",
              "evidence_predicate": {"field": "...", "operator": "gte", "threshold": 1.0}
            }
          }
        }
    """
    uri = watch.get("accreditation_config_uri", "")
    if not uri:
        return []

    try:
        config = load_config_from_uri(uri)
    except Exception as exc:
        logging.warning(json.dumps({
            "msg": "accreditation: failed to load config",
            "uri": uri,
            "error": str(exc),
        }))
        return []

    standards = config.get("standards", {})
    gaps: list = []

    for standard_id, standard in standards.items():
        predicate = standard.get("evidence_predicate")
        if not predicate:
            continue
        satisfied = _evaluate_condition(predicate, rows)
        if not satisfied:
            gaps.append({
                "standard_id": standard_id,
                "description": standard.get("description", ""),
                "evidence_predicate": predicate,
            })

    return gaps


def _run_compliance_watch(watch: dict, rows: list, prev_run_id: str | None) -> list:
    """Evaluate compliance rules against excavation results.

    Loads the compliance ruleset from ``watch["compliance_ruleset_uri"]`` (S3 URI).
    Evaluates each rule type against the result rows. For each detected gap,
    generates draft amendment text via Router ``summarize`` (fail-open).

    Supported rule types:
    - ``international_site``: any row with a non-empty ``country_field`` value → gap
    - ``new_data_source``: any row with a non-empty ``source_id_field`` value → gap
    - ``subject_count``: total row count increased above ``threshold`` fraction vs
      previous run (if available); otherwise flags any rows with a count field present
    - ``classification_change``: any row whose ``classification_field`` differs from
      ``watch["compliance_baseline"]`` dict (if set), otherwise any row with the field

    Returns a list of gap dicts:
    ``[{gap_type, rule_id, affected_record_ids, severity, draft_amendment_text}]``
    """
    ruleset_uri = watch.get("compliance_ruleset_uri", "")
    if not ruleset_uri:
        return []

    try:
        config = load_config_from_uri(ruleset_uri)
    except Exception as exc:
        logging.warning(json.dumps({
            "msg": "compliance_watch: failed to load ruleset",
            "uri": ruleset_uri,
            "error": str(exc),
        }))
        return []

    rules = config.get("rules", [])
    gaps: list = []
    watch_id = watch.get("watch_id", "")
    baseline: dict = watch.get("compliance_baseline", {})

    for rule in rules:
        rule_id = rule.get("rule_id", "")
        rule_type = rule.get("type", "")
        severity = rule.get("severity", "medium")
        affected: list = []

        if rule_type == "international_site":
            field = rule.get("country_field", "country")
            affected = [
                str(row.get("id", i))
                for i, row in enumerate(rows)
                if row.get(field)
            ]

        elif rule_type == "new_data_source":
            field = rule.get("source_id_field", "source_id")
            affected = [
                str(row.get("id", i))
                for i, row in enumerate(rows)
                if row.get(field)
            ]

        elif rule_type == "subject_count":
            count_field = rule.get("count_field", "subject_count")
            threshold = float(rule.get("threshold", 0.10))
            current_total = sum(
                float(row.get(count_field, 0)) for row in rows if row.get(count_field) is not None
            )
            prev_total = float(baseline.get("subject_count_total", 0))
            if prev_total > 0:
                increase_pct = (current_total - prev_total) / prev_total
                if increase_pct > threshold:
                    affected = [str(row.get("id", i)) for i, row in enumerate(rows)]
            elif current_total > 0:
                affected = [str(row.get("id", i)) for i, row in enumerate(rows)]

        elif rule_type == "classification_change":
            class_field = rule.get("classification_field", "data_class")
            baseline_val = baseline.get(class_field)
            if baseline_val is not None:
                affected = [
                    str(row.get("id", i))
                    for i, row in enumerate(rows)
                    if row.get(class_field) is not None and row.get(class_field) != baseline_val
                ]
            else:
                affected = [
                    str(row.get("id", i))
                    for i, row in enumerate(rows)
                    if row.get(class_field) is not None
                ]

        if not affected:
            continue

        # Draft amendment text via Router (fail-open)
        gap_prompt = (
            f"Draft a brief compliance amendment notice for the following gap:\n"
            f"Rule: {rule_type} ({rule_id}), Severity: {severity}\n"
            f"Affected records: {len(affected)}"
        )
        draft_amendment_text = ""
        try:
            result = call_router("summarize", gap_prompt, max_tokens=200)
            if result:
                draft_amendment_text = result.strip()
        except Exception as exc:
            logging.warning(json.dumps({
                "msg": "compliance_watch: Router draft failed",
                "rule_id": rule_id,
                "watch_id": watch_id,
                "error": str(exc),
            }))

        gaps.append({
            "gap_type": rule_type,
            "rule_id": rule_id,
            "affected_record_ids": affected,
            "severity": severity,
            "draft_amendment_text": draft_amendment_text,
        })

    return gaps


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

    # Re-evaluate plan status at execution time (#79).
    # The watch runner is not an AgentCore Gateway tool — Cedar is not evaluated here.
    # Honoring the structural plan status (set by Cedar at creation time) ensures that
    # plans requiring IRB approval (pending_approval) or template plans are never executed
    # by the scheduler even if the watch was created before the plan status changed.
    plan_status = plan.get("status", "ready")
    if plan_status not in ("ready", "approved"):
        _mark_errored(watch_id, watch, f"Plan status '{plan_status}' is not executable")
        return {"status": "error", "error": "Plan is not in an executable state"}

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

    # Feed watches: merge new results into the accumulated dataset
    watch_type = watch.get("type", "alert")
    feed_result_uri = None
    if watch_type == "feed":
        feed_result_uri = _run_feed_merge(watch, watch_id, run_id, rows)

    # Drift condition: compare new results against previous run_id result
    diff_summary = None
    condition = watch.get("condition")
    if condition and condition.get("type") == "drift":
        triggered, diff_summary = _evaluate_drift_condition(condition, run_id, watch)
    else:
        triggered = _evaluate_condition(condition, rows)
    triggered_at = now if triggered else watch.get("last_triggered_at")

    # New-award semantic match: score rows by similarity to lab profile
    new_award_matches: list = []
    if watch.get("type") == "new_award":
        semantic_cfg = watch.get("semantic_match", {})
        new_award_matches = _run_new_award_semantic_match(rows, semantic_cfg)
        # Override triggered: only fire if there are semantically matching awards
        triggered = bool(new_award_matches)
        triggered_at = now if triggered else watch.get("last_triggered_at")

    if triggered and watch.get("notification_target"):
        notify_rows = new_award_matches if watch.get("type") == "new_award" else rows
        _fire_notification(watch["notification_target"], run_id, notify_rows, watch_id)

    # Action routing: draft response via Router and dispatch to configured destination
    if triggered and watch.get("action_routing"):
        _run_action_routing(watch, rows, diff_summary, run_id)

    # Accreditation evidence evaluation
    accreditation_gaps: list = []
    if watch.get("accreditation_config_uri"):
        accreditation_gaps = _evaluate_accreditation(watch, rows)

    # Compliance surface watch
    compliance_gaps: list = []
    if watch.get("compliance_mode"):
        compliance_gaps = _run_compliance_watch(watch, rows, watch.get("last_run_id"))

    watch_updates: dict = {
        "last_run_id": run_id,
        "last_run_at": now,
        "last_triggered_at": triggered_at,
        "consecutive_errors": 0,
        "status": "active",
    }
    if feed_result_uri:
        watch_updates["feed_result_uri"] = feed_result_uri
    update_watch(watch_id, watch_updates)

    audit_out: dict = {
        "status": "complete",
        "watch_id": watch_id,
        "run_id": run_id,
        "rows_returned": len(rows),
        "triggered": triggered,
    }
    if diff_summary is not None:
        audit_out["diff_summary"] = diff_summary
    if new_award_matches:
        audit_out["new_award_matches"] = len(new_award_matches)
    if accreditation_gaps:
        audit_out["accreditation_gaps"] = len(accreditation_gaps)
    if compliance_gaps:
        audit_out["compliance_gaps"] = len(compliance_gaps)

    audit_log("watch-runner", "watch-scheduler", {"watch_id": watch_id}, audit_out)

    result_out: dict = {
        "status": "complete",
        "watch_id": watch_id,
        "run_id": run_id,
        "triggered": triggered,
    }
    if diff_summary is not None:
        result_out["diff_summary"] = diff_summary
    if new_award_matches:
        result_out["new_award_matches"] = len(new_award_matches)
    if accreditation_gaps:
        result_out["accreditation_gaps"] = accreditation_gaps
    if compliance_gaps:
        result_out["compliance_gaps"] = compliance_gaps
    return result_out


def _run_feed_merge(watch: dict, watch_id: str, run_id: str, rows: list[dict]) -> str | None:
    """Merge new excavation rows into the feed's accumulated dataset.

    On the first run (no feed_result_uri yet), stores the new rows as the
    initial feed result and returns the URI. On subsequent runs, calls
    refine with mode="merge" to accumulate without duplicates.

    Returns the URI of the (updated) feed result, or None on error.
    """
    from tools.refine.handler import handler as refine_handler  # noqa: PLC0415

    dedup_key = watch.get("feed_dedup_key", "")
    existing_uri = watch.get("feed_result_uri")

    if not existing_uri:
        # First run — store new rows as the initial feed dataset and record URI
        import boto3 as _boto3  # noqa: PLC0415
        bucket = RUNS_BUCKET
        key = f"feeds/{watch_id}/feed.json"
        import json as _json  # noqa: PLC0415
        _boto3.client("s3").put_object(
            Bucket=bucket,
            Key=key,
            Body=_json.dumps(rows, default=str),
            ContentType="application/json",
        )
        return f"s3://{bucket}/{key}"

    # Subsequent runs — merge via refine handler
    merge_event = {
        "mode": "merge",
        "run_id": run_id,
        "result_s3_uri": existing_uri,
        "dedup_key": dedup_key,
        "output_s3_uri": existing_uri,  # write back to same URI (overwrite feed)
    }
    try:
        result = refine_handler(merge_event, None)
        if result.get("statusCode") == 200:
            import json as _json  # noqa: PLC0415
            body = _json.loads(result["body"])
            return body.get("output_s3_uri", existing_uri)
    except Exception as exc:
        print(_json.dumps({"level": "warn", "msg": "feed merge failed",
                           "watch_id": watch_id, "error": str(exc)}))
    return existing_uri  # Return existing URI even on error — don't lose it


def _evaluate_drift_condition(
    condition: dict, run_id: str, watch: dict
) -> tuple[bool, dict | None]:
    """Evaluate a drift condition by comparing the new result against the previous run.

    Returns (triggered: bool, diff_summary: dict | None).
    - First run (no last_run_id): always returns (False, None) — no baseline yet.
    - Subsequent runs: calls diff_results; fires if change% > threshold_pct.
    """
    from tools.shared import diff_results  # noqa: PLC0415

    prev_run_id = watch.get("last_run_id")
    if not prev_run_id:
        # No previous run to compare against — store without firing
        return False, None

    key_column = condition.get("key_column", "id")
    threshold_pct = float(condition.get("threshold_pct", 10.0))

    # Construct S3 URIs for previous and current result
    uri_prev = f"s3://{RUNS_BUCKET}/{prev_run_id}/result.json"
    uri_curr = f"s3://{RUNS_BUCKET}/{run_id}/result.json"

    try:
        diff = diff_results(uri_prev, uri_curr, key_column)
    except Exception as exc:
        print(json.dumps({"level": "warn", "msg": "drift diff failed",
                          "watch_id": watch.get("watch_id", ""), "error": str(exc)}))
        return False, None

    total = (
        diff["added_count"] + diff["removed_count"] +
        diff["changed_count"] + diff["unchanged_count"]
    )
    if total == 0:
        return False, diff

    change_count = diff["added_count"] + diff["removed_count"] + diff["changed_count"]
    change_pct = (change_count / total) * 100.0
    triggered = change_pct > threshold_pct
    return triggered, diff


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
            import json as _json

            import boto3
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
