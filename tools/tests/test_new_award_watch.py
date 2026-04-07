"""
Tests for clAWS v0.15.0 new-award intelligence watch (#70).

Covers:
- Watch handler validation: watch_type="new_award" requires semantic_match
- Runner _run_new_award_semantic_match: SSM fetch, Router scoring, threshold filter
- Discover: nih-reporter and nsf-awards domains dispatch to _discover_registry with filter
"""

import json  # noqa: I001
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# TestNewAwardWatchValidation
# ---------------------------------------------------------------------------

class TestNewAwardWatchValidation:
    """Watch handler validates new_award type requirements."""

    def _create(self, body: dict) -> dict:
        from tools.watch.handler import _create
        return _create(body, "user:alice", "req-001")

    def test_new_award_without_semantic_match_returns_error(self):
        mock_plan = {
            "plan_id": "plan-abc12345",
            "source_id": "nih:awards",
            "query": "SELECT * FROM awards LIMIT 10",
            "status": "ready",
        }
        with patch("tools.watch.handler.load_plan", return_value=mock_plan), \
             patch("tools.watch.handler.store_watch"), \
             patch("tools.watch.handler._create_schedule"):
            result = self._create({
                "plan_id": "plan-abc12345",
                "schedule": "rate(1 day)",
                "type": "new_award",
                # no semantic_match
            })
        assert result["statusCode"] == 400
        body = json.loads(result["body"])
        assert "semantic_match" in body["error"]

    def test_new_award_without_ssm_key_returns_error(self):
        mock_plan = {
            "plan_id": "plan-abc12345",
            "source_id": "nih:awards",
            "query": "SELECT * FROM awards",
            "status": "ready",
        }
        with patch("tools.watch.handler.load_plan", return_value=mock_plan), \
             patch("tools.watch.handler.store_watch"), \
             patch("tools.watch.handler._create_schedule"):
            result = self._create({
                "plan_id": "plan-abc12345",
                "schedule": "rate(1 day)",
                "type": "new_award",
                "semantic_match": {"abstract_similarity_threshold": 0.8},  # missing lab_profile_ssm_key
            })
        assert result["statusCode"] == 400
        body = json.loads(result["body"])
        assert "lab_profile_ssm_key" in body["error"]

    def test_new_award_with_valid_semantic_match_creates_successfully(self):
        mock_plan = {
            "plan_id": "plan-abc12345",
            "source_id": "nih:awards",
            "query": "SELECT * FROM awards",
            "status": "ready",
        }
        with patch("tools.watch.handler.load_plan", return_value=mock_plan), \
             patch("tools.watch.handler.store_watch") as mock_store, \
             patch("tools.watch.handler._create_schedule"), \
             patch("tools.watch.handler.audit_log"), \
             patch("tools.watch.handler.new_watch_id", return_value="watch-newtest01"):
            result = self._create({
                "plan_id": "plan-abc12345",
                "schedule": "rate(1 day)",
                "type": "new_award",
                "semantic_match": {
                    "lab_profile_ssm_key": "/quick-suite/claws/lab-profile",
                    "abstract_similarity_threshold": 0.82,
                },
            })
        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["watch_id"] == "watch-newtest01"
        # Confirm semantic_match stored in spec
        stored_spec = mock_store.call_args[0][1]
        assert stored_spec["semantic_match"]["lab_profile_ssm_key"] == "/quick-suite/claws/lab-profile"
        assert stored_spec["type"] == "new_award"

    def test_invalid_watch_type_returns_error(self):
        mock_plan = {"plan_id": "plan-abc12345", "source_id": "x", "query": "x", "status": "ready"}
        with patch("tools.watch.handler.load_plan", return_value=mock_plan):
            result = self._create({
                "plan_id": "plan-abc12345",
                "schedule": "rate(1 day)",
                "type": "invalid_type",
            })
        assert result["statusCode"] == 400


# ---------------------------------------------------------------------------
# TestNewAwardWatchRunner
# ---------------------------------------------------------------------------

_SAMPLE_AWARDS = [
    {
        "id": "R01CA123",
        "title": "Machine Learning for Cancer Detection",
        "abstract_text": "This project uses deep learning to detect cancer in imaging data.",
    },
    {
        "id": "R01GM456",
        "title": "Genomics of Rare Diseases",
        "abstract_text": "Genome-wide association studies for rare pediatric diseases.",
    },
]

_LAB_PROFILE = "Our lab develops machine learning methods for early disease detection using medical imaging."


class TestNewAwardWatchRunner:
    """Tests for _run_new_award_semantic_match and integration in runner handler."""

    def _run_semantic_match(self, rows, cfg, router_scores=None, ssm_value=_LAB_PROFILE, ssm_error=False):
        """Helper: run _run_new_award_semantic_match with mocked SSM and Router."""
        from tools.watch.runner import _run_new_award_semantic_match

        mock_ssm = MagicMock()
        if ssm_error:
            mock_ssm.get_parameter.side_effect = Exception("SSM unavailable")
        else:
            mock_ssm.get_parameter.return_value = {"Parameter": {"Value": ssm_value}}

        scores = iter(router_scores or [0.9, 0.3])

        def mock_call_router(tool, prompt, max_tokens=10):
            return str(next(scores, "0.0"))

        with patch("tools.watch.runner._ssm", return_value=mock_ssm), \
             patch("tools.watch.runner.call_router", side_effect=mock_call_router):
            return _run_new_award_semantic_match(rows, cfg)

    def test_above_threshold_award_returned(self):
        cfg = {
            "lab_profile_ssm_key": "/quick-suite/claws/lab-profile",
            "abstract_similarity_threshold": 0.80,
        }
        result = self._run_semantic_match(_SAMPLE_AWARDS, cfg, router_scores=[0.91, 0.25])
        assert len(result) == 1
        assert result[0]["id"] == "R01CA123"
        assert result[0]["_similarity_score"] == 0.91

    def test_below_threshold_award_not_returned(self):
        cfg = {
            "lab_profile_ssm_key": "/quick-suite/claws/lab-profile",
            "abstract_similarity_threshold": 0.90,
        }
        result = self._run_semantic_match(_SAMPLE_AWARDS, cfg, router_scores=[0.85, 0.70])
        assert len(result) == 0

    def test_empty_rows_returns_empty(self):
        cfg = {"lab_profile_ssm_key": "/quick-suite/claws/lab-profile"}
        result = self._run_semantic_match([], cfg, router_scores=[])
        assert result == []

    def test_ssm_failure_logs_warning_and_returns_empty(self):
        cfg = {"lab_profile_ssm_key": "/quick-suite/claws/lab-profile"}
        result = self._run_semantic_match(_SAMPLE_AWARDS, cfg, ssm_error=True)
        assert result == []

    def test_router_failure_skips_award_without_crash(self):
        from tools.watch.runner import _run_new_award_semantic_match

        mock_ssm = MagicMock()
        mock_ssm.get_parameter.return_value = {"Parameter": {"Value": _LAB_PROFILE}}

        call_count = [0]

        def mock_call_router(tool, prompt, max_tokens=10):
            call_count[0] += 1
            if call_count[0] == 1:
                raise RuntimeError("Router timeout")
            return "0.95"

        with patch("tools.watch.runner._ssm", return_value=mock_ssm), \
             patch("tools.watch.runner.call_router", side_effect=mock_call_router):
            result = _run_new_award_semantic_match(_SAMPLE_AWARDS, {
                "lab_profile_ssm_key": "/q/p",
                "abstract_similarity_threshold": 0.80,
            })
        # First award skipped (Router error), second award matches
        assert len(result) == 1
        assert result[0]["id"] == "R01GM456"

    def test_max_50_rows_enforced(self):
        """Only the first 50 rows are scored even if more are provided."""
        rows = [{"id": str(i), "abstract_text": "test abstract"} for i in range(100)]
        cfg = {"lab_profile_ssm_key": "/quick-suite/claws/lab-profile", "abstract_similarity_threshold": 0.5}

        scored_count = [0]

        mock_ssm = MagicMock()
        mock_ssm.get_parameter.return_value = {"Parameter": {"Value": "lab profile"}}

        def counting_router(tool, prompt, max_tokens=10):
            scored_count[0] += 1
            return "0.9"

        from tools.watch.runner import _run_new_award_semantic_match
        with patch("tools.watch.runner._ssm", return_value=mock_ssm), \
             patch("tools.watch.runner.call_router", side_effect=counting_router):
            _run_new_award_semantic_match(rows, cfg)

        assert scored_count[0] == 50  # exactly 50 scored

    def test_runner_handler_new_award_triggers_on_matches(self):
        """Full handler integration: new_award watch fires notification when matches found."""
        from tools.watch.runner import handler

        mock_watch = {
            "watch_id": "watch-na001",
            "plan_id": "plan-na001",
            "status": "active",
            "type": "new_award",
            "consecutive_errors": 0,
            "notification_target": {"type": "s3", "uri": "s3://bucket/alerts/"},
            "semantic_match": {
                "lab_profile_ssm_key": "/quick-suite/claws/lab-profile",
                "abstract_similarity_threshold": 0.80,
            },
        }
        mock_plan = {
            "plan_id": "plan-na001",
            "source_id": "athena:db.awards",
            "query": "SELECT * FROM awards LIMIT 10",
            "query_type": "athena_sql",
            "status": "ready",
            "constraints": {},
        }
        mock_executor_result = {
            "status": "ok",
            "rows": _SAMPLE_AWARDS,
            "bytes_scanned": 1000,
            "cost": "$0.01",
        }

        with patch("tools.watch.runner.load_watch", return_value=mock_watch), \
             patch("tools.watch.runner.load_plan", return_value=mock_plan), \
             patch("tools.watch.runner.update_watch"), \
             patch("tools.watch.runner.store_result"), \
             patch("tools.watch.runner.store_result_metadata"), \
             patch("tools.watch.runner.new_run_id", return_value="run-na001"), \
             patch("tools.watch.runner.audit_log"), \
             patch("tools.watch.runner._fire_notification") as mock_fire, \
             patch("tools.watch.runner._run_new_award_semantic_match", return_value=[
                 {**_SAMPLE_AWARDS[0], "_similarity_score": 0.91}
             ]):
            from tools.excavate.handler import EXECUTORS
            mock_exec = MagicMock(return_value=mock_executor_result)
            original = EXECUTORS.get("athena_sql")
            try:
                EXECUTORS["athena_sql"] = mock_exec
                result = handler({"watch_id": "watch-na001"}, None)
            finally:
                if original:
                    EXECUTORS["athena_sql"] = original
                else:
                    EXECUTORS.pop("athena_sql", None)

        assert result["triggered"] is True
        assert result["new_award_matches"] == 1
        mock_fire.assert_called_once()

    def test_runner_handler_new_award_no_matches_does_not_fire(self):
        """New_award watch does not fire notification when no matches found."""
        from tools.watch.runner import handler

        mock_watch = {
            "watch_id": "watch-na002",
            "plan_id": "plan-na002",
            "status": "active",
            "type": "new_award",
            "consecutive_errors": 0,
            "notification_target": {"type": "s3", "uri": "s3://bucket/alerts/"},
            "semantic_match": {"lab_profile_ssm_key": "/quick-suite/claws/lab-profile"},
        }
        mock_plan = {
            "plan_id": "plan-na002",
            "source_id": "athena:db.awards",
            "query": "SELECT * FROM awards LIMIT 10",
            "query_type": "athena_sql",
            "status": "ready",
            "constraints": {},
        }

        with patch("tools.watch.runner.load_watch", return_value=mock_watch), \
             patch("tools.watch.runner.load_plan", return_value=mock_plan), \
             patch("tools.watch.runner.update_watch"), \
             patch("tools.watch.runner.store_result"), \
             patch("tools.watch.runner.store_result_metadata"), \
             patch("tools.watch.runner.new_run_id", return_value="run-na002"), \
             patch("tools.watch.runner.audit_log"), \
             patch("tools.watch.runner._fire_notification") as mock_fire, \
             patch("tools.watch.runner._run_new_award_semantic_match", return_value=[]):
            from tools.excavate.handler import EXECUTORS
            EXECUTORS["athena_sql"] = MagicMock(return_value={"status": "ok", "rows": _SAMPLE_AWARDS})
            result = handler({"watch_id": "watch-na002"}, None)

        assert result["triggered"] is False
        mock_fire.assert_not_called()


# ---------------------------------------------------------------------------
# TestDiscoverDomainAllowlist
# ---------------------------------------------------------------------------

class TestDiscoverDomainAllowlist:
    """nih-reporter and nsf-awards domains dispatch to _discover_registry with type filter."""

    def _discover(self, domains: list[str]) -> dict:
        from tools.discover.handler import handler
        event = {"query": "machine learning cancer", "scope": {"domains": domains}}
        with patch("tools.discover.handler.audit_log"):
            return handler(event, None)

    def test_nih_reporter_domain_calls_registry_with_filter(self):
        with patch("tools.discover.handler._discover_registry") as mock_reg, \
             patch("tools.discover.handler.audit_log"):
            mock_reg.return_value = []
            from tools.discover.handler import handler
            handler({"query": "cancer", "scope": {"domains": ["nih-reporter"]}}, None)
        # Should be called with source_type_filter="nih_reporter"
        calls = [call for call in mock_reg.call_args_list]
        assert any(
            call.kwargs.get("source_type_filter") == "nih_reporter" or
            (len(call.args) >= 3 and call.args[2] == "nih_reporter")
            for call in calls
        )

    def test_nsf_awards_domain_calls_registry_with_filter(self):
        with patch("tools.discover.handler._discover_registry") as mock_reg, \
             patch("tools.discover.handler.audit_log"):
            mock_reg.return_value = []
            from tools.discover.handler import handler
            handler({"query": "climate", "scope": {"domains": ["nsf-awards"]}}, None)
        calls = [call for call in mock_reg.call_args_list]
        assert any(
            call.kwargs.get("source_type_filter") == "nsf_awards" or
            (len(call.args) >= 3 and call.args[2] == "nsf_awards")
            for call in calls
        )

    def test_both_research_domains_in_single_request(self):
        with patch("tools.discover.handler._discover_registry") as mock_reg, \
             patch("tools.discover.handler.audit_log"):
            mock_reg.return_value = []
            from tools.discover.handler import handler
            handler({"query": "research", "scope": {"domains": ["nih-reporter", "nsf-awards"]}}, None)
        # Two calls expected — one per domain
        assert mock_reg.call_count == 2

    def test_registry_domain_still_works_unfiltered(self):
        with patch("tools.discover.handler._discover_registry") as mock_reg, \
             patch("tools.discover.handler.audit_log"):
            mock_reg.return_value = [{"id": "src-1", "confidence": 0.8}]
            from tools.discover.handler import handler
            result = handler({"query": "data", "scope": {"domains": ["registry"]}}, None)
        body = json.loads(result["body"])
        assert body["sources"] is not None
        # Called with no filter (or filter=None)
        call_args = mock_reg.call_args
        filter_val = call_args.kwargs.get("source_type_filter", call_args.args[2] if len(call_args.args) > 2 else None)
        assert filter_val is None
