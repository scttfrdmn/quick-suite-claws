"""
Tests for clAWS v0.15.0 completion:
- #68 Watch action routing (action_routing field, Router draft, SNS/EventBridge dispatch)
- #67 Accreditation evidence ledger (load_config_from_uri, AccreditationConfig, gap detection)
- #69 Compliance surface watch (compliance_mode validation, rule evaluation, Router draft)
"""

import json  # noqa: I001
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# TestLoadConfigFromUri
# ---------------------------------------------------------------------------

class TestLoadConfigFromUri:
    """shared.load_config_from_uri loads JSON from S3 or SSM URIs."""

    def test_s3_uri_loads_json(self):
        from tools.shared import load_config_from_uri
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {
            "Body": MagicMock(read=lambda: b'{"standards": {"S1": {"description": "test"}}}')
        }
        with patch("tools.shared.s3_client", return_value=mock_s3):
            result = load_config_from_uri("s3://my-bucket/configs/accred.json")
        assert result["standards"]["S1"]["description"] == "test"
        mock_s3.get_object.assert_called_once_with(Bucket="my-bucket", Key="configs/accred.json")

    def test_ssm_uri_loads_json(self):
        from tools.shared import load_config_from_uri
        mock_ssm = MagicMock()
        mock_ssm.get_parameter.return_value = {
            "Parameter": {"Value": '{"rules": []}'}
        }
        with patch("tools.shared.ssm_client", return_value=mock_ssm):
            result = load_config_from_uri("ssm:/quick-suite/claws/compliance-rules")
        assert result == {"rules": []}
        mock_ssm.get_parameter.assert_called_once_with(Name="/quick-suite/claws/compliance-rules")

    def test_invalid_scheme_raises_value_error(self):
        import pytest  # noqa: I001
        from tools.shared import load_config_from_uri
        with pytest.raises(ValueError, match="Unsupported config URI scheme"):
            load_config_from_uri("https://example.com/config.json")

    def test_invalid_s3_uri_raises_value_error(self):
        import pytest  # noqa: I001
        from tools.shared import load_config_from_uri
        with pytest.raises(ValueError, match="Invalid S3 URI"):
            load_config_from_uri("s3://bucket-only")


# ---------------------------------------------------------------------------
# TestWatchActionRouting — watch creation validation
# ---------------------------------------------------------------------------

class TestWatchActionRouting:
    """Watch handler validates and stores action_routing config."""

    def _create(self, body: dict) -> dict:
        from tools.watch.handler import _create
        return _create(body, "user:alice", "req-001")

    def test_action_routing_stored_on_create(self):
        mock_plan = {"plan_id": "plan-ar001", "source_id": "athena:db.t", "query": "SELECT 1", "status": "ready"}
        with patch("tools.watch.handler.load_plan", return_value=mock_plan), \
             patch("tools.watch.handler.store_watch") as mock_store, \
             patch("tools.watch.handler._create_schedule"), \
             patch("tools.watch.handler.audit_log"), \
             patch("tools.watch.handler.new_watch_id", return_value="watch-ar001"):
            result = self._create({
                "plan_id": "plan-ar001",
                "schedule": "rate(1 day)",
                "action_routing": {
                    "destination_type": "sns",
                    "destination_arn": "arn:aws:sns:us-east-1:123:my-topic",
                    "context_template": "Drift detected: {diff_summary}",
                },
            })
        assert result["statusCode"] == 200
        stored = mock_store.call_args[0][1]
        assert stored["action_routing"]["destination_type"] == "sns"

    def test_invalid_destination_type_returns_error(self):
        mock_plan = {"plan_id": "plan-ar002", "source_id": "x", "query": "x", "status": "ready"}
        with patch("tools.watch.handler.load_plan", return_value=mock_plan):
            result = self._create({
                "plan_id": "plan-ar002",
                "schedule": "rate(1 day)",
                "action_routing": {"destination_type": "slack", "destination_arn": "arn:xxx"},
            })
        assert result["statusCode"] == 400
        assert "destination_type" in json.loads(result["body"])["error"]

    def test_missing_destination_arn_returns_error(self):
        mock_plan = {"plan_id": "plan-ar003", "source_id": "x", "query": "x", "status": "ready"}
        with patch("tools.watch.handler.load_plan", return_value=mock_plan):
            result = self._create({
                "plan_id": "plan-ar003",
                "schedule": "rate(1 day)",
                "action_routing": {"destination_type": "eventbridge"},
            })
        assert result["statusCode"] == 400


# ---------------------------------------------------------------------------
# TestRunActionRouting — runner function
# ---------------------------------------------------------------------------

_SAMPLE_DIFF = {"added_count": 5, "removed_count": 0, "changed_count": 2, "unchanged_count": 10}


class TestRunActionRouting:
    """Tests for _run_action_routing in runner."""

    def _run(self, watch, rows, diff_summary, run_id, router_response="Draft text.", sns_error=False):
        from tools.watch.runner import _run_action_routing
        with patch("tools.watch.runner.call_router", return_value=router_response), \
             patch("boto3.client") as mock_client:
            mock_sns = MagicMock()
            if sns_error:
                mock_sns.publish.side_effect = Exception("SNS unavailable")
            mock_client.return_value = mock_sns
            _run_action_routing(watch, rows, diff_summary, run_id)
        return mock_client, mock_sns

    def test_sns_dispatch_called_with_payload(self):
        watch = {
            "watch_id": "watch-ar1",
            "action_routing": {
                "destination_type": "sns",
                "destination_arn": "arn:aws:sns:us-east-1:123:topic",
                "context_template": "Changes: {added_count} added",
            },
        }
        mock_client, mock_sns = self._run(watch, [{"id": "1"}], _SAMPLE_DIFF, "run-001")
        mock_sns.publish.assert_called_once()
        call_kwargs = mock_sns.publish.call_args[1]
        assert call_kwargs["TopicArn"] == "arn:aws:sns:us-east-1:123:topic"
        payload = json.loads(call_kwargs["Message"])
        assert payload["watch_id"] == "watch-ar1"
        assert payload["run_id"] == "run-001"
        assert payload["draft_text"] == "Draft text."

    def test_template_substitution_applied(self):
        watch = {
            "watch_id": "watch-ar2",
            "action_routing": {
                "destination_type": "sns",
                "destination_arn": "arn:aws:sns:us-east-1:123:topic",
                "context_template": "{added_count} records added",
            },
        }
        captured_prompts = []

        def capture_router(tool, prompt, max_tokens=10):
            captured_prompts.append(prompt)
            return "Draft."

        from tools.watch.runner import _run_action_routing
        with patch("tools.watch.runner.call_router", side_effect=capture_router), \
             patch("boto3.client") as mock_client:
            mock_client.return_value = MagicMock()
            _run_action_routing(watch, [], _SAMPLE_DIFF, "run-002")

        assert "5 records added" in captured_prompts[0]

    def test_router_failure_delivers_without_draft(self):
        watch = {
            "watch_id": "watch-ar3",
            "action_routing": {
                "destination_type": "sns",
                "destination_arn": "arn:aws:sns:us-east-1:123:topic",
                "context_template": "Alert",
            },
        }
        from tools.watch.runner import _run_action_routing
        with patch("tools.watch.runner.call_router", side_effect=RuntimeError("timeout")), \
             patch("boto3.client") as mock_client:
            mock_sns = MagicMock()
            mock_client.return_value = mock_sns
            _run_action_routing(watch, [{"id": "1"}], None, "run-003")

        mock_sns.publish.assert_called_once()
        payload = json.loads(mock_sns.publish.call_args[1]["Message"])
        assert payload["draft_text"] is None

    def test_no_action_routing_field_noop(self):
        watch = {"watch_id": "watch-ar4"}  # no action_routing
        from tools.watch.runner import _run_action_routing
        with patch("boto3.client") as mock_client:
            _run_action_routing(watch, [], None, "run-004")
        mock_client.assert_not_called()

    def test_sns_delivery_failure_is_non_blocking(self):
        watch = {
            "watch_id": "watch-ar5",
            "action_routing": {
                "destination_type": "sns",
                "destination_arn": "arn:xxx",
                "context_template": "",
            },
        }
        from tools.watch.runner import _run_action_routing
        with patch("tools.watch.runner.call_router", return_value=None), \
             patch("boto3.client") as mock_client:
            mock_client.return_value.publish.side_effect = Exception("SNS error")
            # Should not raise
            _run_action_routing(watch, [], None, "run-005")


# ---------------------------------------------------------------------------
# TestAccreditationEvidenceLedger
# ---------------------------------------------------------------------------

_ACCRED_CONFIG = {
    "standards": {
        "SACSCOC-8.2.c": {
            "description": "Faculty credential verification",
            "evidence_predicate": {"field": "verified_pct", "operator": "gte", "threshold": 1.0},
        },
        "HLC-4.A.1": {
            "description": "Assessment plan existence",
            "evidence_predicate": {"field": "plan_count", "operator": "gt", "threshold": 0},
        },
    }
}


class TestAccreditationEvidenceLedger:
    """Tests for _evaluate_accreditation in runner."""

    def _evaluate(self, rows, config=_ACCRED_CONFIG, load_error=False):
        from tools.watch.runner import _evaluate_accreditation
        watch = {"accreditation_config_uri": "s3://bucket/accred.json", "watch_id": "w-1"}
        if load_error:
            with patch("tools.watch.runner.load_config_from_uri", side_effect=Exception("S3 error")):
                return _evaluate_accreditation(watch, rows)
        with patch("tools.watch.runner.load_config_from_uri", return_value=config):
            return _evaluate_accreditation(watch, rows)

    def test_all_predicates_satisfied_returns_empty_gaps(self):
        rows = [{"verified_pct": 1.0, "plan_count": 3}]
        gaps = self._evaluate(rows)
        assert gaps == []

    def test_failed_predicate_returns_gap(self):
        rows = [{"verified_pct": 0.85, "plan_count": 3}]
        gaps = self._evaluate(rows)
        assert len(gaps) == 1
        assert gaps[0]["standard_id"] == "SACSCOC-8.2.c"
        assert "Faculty credential" in gaps[0]["description"]

    def test_multiple_failing_standards_all_returned(self):
        rows = [{"verified_pct": 0.5, "plan_count": 0}]
        gaps = self._evaluate(rows)
        assert len(gaps) == 2
        ids = {g["standard_id"] for g in gaps}
        assert "SACSCOC-8.2.c" in ids
        assert "HLC-4.A.1" in ids

    def test_config_load_failure_returns_empty_list(self):
        gaps = self._evaluate([], load_error=True)
        assert gaps == []

    def test_missing_accreditation_uri_returns_empty(self):
        from tools.watch.runner import _evaluate_accreditation
        gaps = _evaluate_accreditation({"watch_id": "w"}, [{"field": "value"}])
        assert gaps == []


# ---------------------------------------------------------------------------
# TestComplianceSurfaceWatch
# ---------------------------------------------------------------------------

_COMPLIANCE_RULESET = {
    "rules": [
        {"rule_id": "intl-01", "type": "international_site", "country_field": "country", "severity": "high"},
        {"rule_id": "src-01",  "type": "new_data_source",    "source_id_field": "src",   "severity": "medium"},
        {"rule_id": "subj-01", "type": "subject_count",      "count_field": "n",          "threshold": 0.10, "severity": "high"},
        {"rule_id": "cls-01",  "type": "classification_change", "classification_field": "cls", "severity": "high"},
    ]
}


class TestComplianceSurfaceWatch:
    """Tests for compliance_mode validation and _run_compliance_watch."""

    def _create_watch(self, body: dict) -> dict:
        from tools.watch.handler import _create
        return _create(body, "user:alice", "req-001")

    def test_compliance_mode_without_ruleset_uri_returns_error(self):
        mock_plan = {"plan_id": "plan-cw01", "source_id": "x", "query": "x", "status": "ready"}
        with patch("tools.watch.handler.load_plan", return_value=mock_plan):
            result = self._create_watch({
                "plan_id": "plan-cw01",
                "schedule": "rate(1 day)",
                "compliance_mode": True,
                # no compliance_ruleset_uri
            })
        assert result["statusCode"] == 400
        assert "compliance_ruleset_uri" in json.loads(result["body"])["error"]

    def test_compliance_mode_with_uri_creates_successfully(self):
        mock_plan = {"plan_id": "plan-cw02", "source_id": "x", "query": "x", "status": "ready"}
        with patch("tools.watch.handler.load_plan", return_value=mock_plan), \
             patch("tools.watch.handler.store_watch") as mock_store, \
             patch("tools.watch.handler._create_schedule"), \
             patch("tools.watch.handler.audit_log"), \
             patch("tools.watch.handler.new_watch_id", return_value="watch-cw02"):
            result = self._create_watch({
                "plan_id": "plan-cw02",
                "schedule": "rate(1 day)",
                "compliance_mode": True,
                "compliance_ruleset_uri": "s3://bucket/ruleset.json",
            })
        assert result["statusCode"] == 200
        stored = mock_store.call_args[0][1]
        assert stored["compliance_mode"] is True
        assert stored["compliance_ruleset_uri"] == "s3://bucket/ruleset.json"

    def _run_compliance(self, rows, ruleset=_COMPLIANCE_RULESET, baseline=None, router_response="Amendment draft."):
        from tools.watch.runner import _run_compliance_watch
        watch = {
            "watch_id": "w-cw",
            "compliance_ruleset_uri": "s3://bucket/ruleset.json",
            "compliance_baseline": baseline or {},
        }
        with patch("tools.watch.runner.load_config_from_uri", return_value=ruleset), \
             patch("tools.watch.runner.call_router", return_value=router_response):
            return _run_compliance_watch(watch, rows, None)

    def test_international_site_rule_detects_non_domestic_rows(self):
        rows = [{"id": "row1", "country": "Germany"}, {"id": "row2", "country": ""}]
        gaps = self._run_compliance(rows)
        intl_gaps = [g for g in gaps if g["gap_type"] == "international_site"]
        assert len(intl_gaps) == 1
        assert "row1" in intl_gaps[0]["affected_record_ids"]
        assert "row2" not in intl_gaps[0]["affected_record_ids"]

    def test_new_data_source_rule_detects_rows_with_source_id(self):
        rows = [{"id": "r1", "src": "dataset-xyz"}, {"id": "r2"}]
        gaps = self._run_compliance(rows)
        src_gaps = [g for g in gaps if g["gap_type"] == "new_data_source"]
        assert len(src_gaps) == 1
        assert "r1" in src_gaps[0]["affected_record_ids"]

    def test_subject_count_increase_above_threshold_triggers_gap(self):
        rows = [{"id": "r1", "n": 120}]
        baseline = {"subject_count_total": 100}
        gaps = self._run_compliance(rows, baseline=baseline)
        subj_gaps = [g for g in gaps if g["gap_type"] == "subject_count"]
        assert len(subj_gaps) == 1

    def test_classification_change_triggers_gap(self):
        rows = [{"id": "r1", "cls": "phi"}, {"id": "r2", "cls": "public"}]
        baseline = {"cls": "public"}
        gaps = self._run_compliance(rows, baseline=baseline)
        cls_gaps = [g for g in gaps if g["gap_type"] == "classification_change"]
        assert len(cls_gaps) == 1
        assert "r1" in cls_gaps[0]["affected_record_ids"]

    def test_all_rules_satisfied_returns_empty_list(self):
        rows = [{"id": "r1"}]  # no country, no src, no n, no cls
        gaps = self._run_compliance(rows)
        assert gaps == []

    def test_router_failure_for_draft_text_returns_empty_string(self):
        from tools.watch.runner import _run_compliance_watch
        rows = [{"id": "r1", "country": "Brazil"}]
        watch = {
            "watch_id": "w-cw2",
            "compliance_ruleset_uri": "s3://bucket/ruleset.json",
            "compliance_baseline": {},
        }
        with patch("tools.watch.runner.load_config_from_uri", return_value=_COMPLIANCE_RULESET), \
             patch("tools.watch.runner.call_router", side_effect=RuntimeError("timeout")):
            gaps = _run_compliance_watch(watch, rows, None)
        intl_gaps = [g for g in gaps if g["gap_type"] == "international_site"]
        assert len(intl_gaps) == 1
        assert intl_gaps[0]["draft_amendment_text"] == ""

    def test_gap_includes_severity_and_draft_text(self):
        rows = [{"id": "r1", "country": "France"}]
        gaps = self._run_compliance(rows, router_response="Please file GDPR check.")
        intl_gaps = [g for g in gaps if g["gap_type"] == "international_site"]
        assert intl_gaps[0]["severity"] == "high"
        assert intl_gaps[0]["draft_amendment_text"] == "Please file GDPR check."
