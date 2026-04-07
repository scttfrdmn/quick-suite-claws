"""
Feature tests for clAWS v0.14.0 (#79, #80, #66).

#79 — Watch runner plan status check (Cedar re-eval at execution time)
#80 — Export destination URI allowlist
#66 — Plan templating with fill-in variables
"""

import json
import os
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# #79 — Watch runner plan status check
# ---------------------------------------------------------------------------

class TestWatchRunnerPlanStatus:
    """Watch runner skips execution when plan is not in an executable state."""

    def _run(self, plan_status: str, watch_status: str = "active") -> dict:
        from tools.watch.runner import handler

        mock_watch = {
            "watch_id": "watch-abc12345",
            "plan_id": "plan-abc12345",
            "status": watch_status,
            "type": "alert",
            "consecutive_errors": 0,
        }
        mock_plan = {
            "plan_id": "plan-abc12345",
            "source_id": "athena:db.patients",
            "query": "SELECT * FROM patients LIMIT 10",
            "query_type": "athena_sql",
            "status": plan_status,
            "constraints": {},
        }

        with patch("tools.watch.runner.load_watch", return_value=mock_watch), \
             patch("tools.watch.runner.load_plan", return_value=mock_plan), \
             patch("tools.watch.runner.update_watch") as mock_update, \
             patch("tools.watch.runner._mark_errored") as mock_errored:
            result = handler({"watch_id": "watch-abc12345"}, None)

        return result, mock_errored, mock_update

    def test_ready_plan_proceeds(self):
        with patch("tools.watch.runner.load_watch") as mock_lw, \
             patch("tools.watch.runner.load_plan") as mock_lp, \
             patch("tools.watch.runner.update_watch"), \
             patch("tools.watch.runner.audit_log"), \
             patch("tools.watch.runner.store_result"), \
             patch("tools.watch.runner.store_result_metadata"), \
             patch("tools.watch.runner.new_run_id", return_value="run-test"):
            mock_lw.return_value = {
                "watch_id": "watch-abc12345", "plan_id": "plan-abc12345",
                "status": "active", "type": "alert", "consecutive_errors": 0,
            }
            mock_lp.return_value = {
                "source_id": "athena:db.patients",
                "query": "SELECT * FROM patients",
                "query_type": "athena_sql",
                "status": "ready",
                "constraints": {},
            }
            mock_exec = MagicMock(return_value={"status": "complete", "rows": []})
            with patch.dict("tools.watch.runner.EXECUTORS", {"athena_sql": mock_exec}):
                from tools.watch.runner import handler
                result = handler({"watch_id": "watch-abc12345"}, None)
        assert result["status"] == "complete"

    def test_approved_plan_proceeds(self):
        """IRB-approved plans (status='approved') are executable by the watch runner."""
        with patch("tools.watch.runner.load_watch") as mock_lw, \
             patch("tools.watch.runner.load_plan") as mock_lp, \
             patch("tools.watch.runner.update_watch"), \
             patch("tools.watch.runner.audit_log"), \
             patch("tools.watch.runner.store_result"), \
             patch("tools.watch.runner.store_result_metadata"), \
             patch("tools.watch.runner.new_run_id", return_value="run-test"):
            mock_lw.return_value = {
                "watch_id": "watch-abc12345", "plan_id": "plan-abc12345",
                "status": "active", "type": "alert", "consecutive_errors": 0,
            }
            mock_lp.return_value = {
                "source_id": "athena:db.patients",
                "query": "SELECT * FROM patients",
                "query_type": "athena_sql",
                "status": "approved",
                "constraints": {},
            }
            mock_exec = MagicMock(return_value={"status": "complete", "rows": []})
            with patch.dict("tools.watch.runner.EXECUTORS", {"athena_sql": mock_exec}):
                from tools.watch.runner import handler
                result = handler({"watch_id": "watch-abc12345"}, None)
        assert result["status"] == "complete"

    def test_pending_approval_plan_blocked(self):
        """Plans with status='pending_approval' must not execute via watch runner."""
        with patch("tools.watch.runner.load_watch") as mock_lw, \
             patch("tools.watch.runner.load_plan") as mock_lp, \
             patch("tools.watch.runner._mark_errored") as mock_errored:
            mock_lw.return_value = {
                "watch_id": "watch-abc12345", "plan_id": "plan-abc12345",
                "status": "active", "type": "alert", "consecutive_errors": 0,
            }
            mock_lp.return_value = {
                "source_id": "athena:db.patients",
                "query": "SELECT * FROM patients",
                "query_type": "athena_sql",
                "status": "pending_approval",
                "constraints": {},
            }
            from tools.watch.runner import handler
            result = handler({"watch_id": "watch-abc12345"}, None)
        assert result["status"] == "error"
        assert "executable" in result["error"]
        mock_errored.assert_called_once()

    def test_template_plan_blocked(self):
        """Plans with status='template' must not execute via watch runner."""
        with patch("tools.watch.runner.load_watch") as mock_lw, \
             patch("tools.watch.runner.load_plan") as mock_lp, \
             patch("tools.watch.runner._mark_errored") as mock_errored:
            mock_lw.return_value = {
                "watch_id": "watch-abc12345", "plan_id": "plan-abc12345",
                "status": "active", "type": "alert", "consecutive_errors": 0,
            }
            mock_lp.return_value = {
                "source_id": "athena:db.patients",
                "query": "",
                "query_type": "",
                "status": "template",
                "constraints": {},
            }
            from tools.watch.runner import handler
            result = handler({"watch_id": "watch-abc12345"}, None)
        assert result["status"] == "error"
        assert "executable" in result["error"]
        mock_errored.assert_called_once()


# ---------------------------------------------------------------------------
# #80 — Export destination allowlist
# ---------------------------------------------------------------------------

class TestExportDestinationAllowlist:
    """_validate_destination_uri enforces allowlist and HTTPS requirement."""

    def _validate(self, dest_type: str, dest_uri: str, allowlist: str = "") -> str | None:
        with patch.dict(os.environ, {"CLAWS_EXPORT_ALLOWED_DESTINATIONS": allowlist}):
            # Re-import to pick up env var (module-level constant)
            import importlib
            import tools.export.handler as exp_mod
            importlib.reload(exp_mod)
            return exp_mod._validate_destination_uri(dest_type, dest_uri)

    def test_no_allowlist_allows_s3(self):
        assert self._validate("s3", "s3://any-bucket/any-key") is None

    def test_no_allowlist_allows_eventbridge(self):
        assert self._validate("eventbridge", "events://my-bus/MyEvent") is None

    def test_no_allowlist_allows_https_callback(self):
        assert self._validate("callback", "https://api.example.com/webhook") is None

    def test_callback_http_rejected_even_without_allowlist(self):
        result = self._validate("callback", "http://internal.example.com/hook")
        assert result is not None
        assert "HTTPS" in result

    def test_allowlist_matching_s3_prefix_allowed(self):
        result = self._validate("s3", "s3://approved-bucket/exports/file.json",
                                allowlist="s3://approved-bucket/")
        assert result is None

    def test_allowlist_non_matching_s3_prefix_rejected(self):
        result = self._validate("s3", "s3://other-bucket/file.json",
                                allowlist="s3://approved-bucket/")
        assert result is not None
        assert "allowlist" in result

    def test_allowlist_matching_events_prefix_allowed(self):
        result = self._validate("eventbridge", "events://internal-bus/ClawsEvent",
                                allowlist="events://internal-bus/")
        assert result is None

    def test_allowlist_multiple_prefixes_second_matches(self):
        result = self._validate("s3", "s3://backup-bucket/data.json",
                                allowlist="s3://primary-bucket/,s3://backup-bucket/")
        assert result is None

    def test_callback_https_matching_allowlist_allowed(self):
        result = self._validate("callback", "https://api.internal.com/hook",
                                allowlist="https://api.internal.com/")
        assert result is None

    def test_callback_https_not_in_allowlist_rejected(self):
        result = self._validate("callback", "https://external.com/hook",
                                allowlist="https://api.internal.com/")
        assert result is not None

    def test_handler_returns_error_when_destination_blocked(self):
        """Export handler propagates allowlist rejection to caller."""
        import importlib
        import tools.export.handler as exp_mod
        with patch.dict(os.environ, {"CLAWS_EXPORT_ALLOWED_DESTINATIONS": "s3://safe-bucket/"}):
            importlib.reload(exp_mod)
            mock_load = MagicMock(return_value=[{"col": "val"}])
            mock_scan = MagicMock(return_value={"status": "clean"})
            with patch.object(exp_mod, "load_result", mock_load), \
                 patch.object(exp_mod, "scan_payload", mock_scan), \
                 patch.object(exp_mod, "audit_log"):
                result = exp_mod.handler({
                    "run_id": "run-test",
                    "destination": {"type": "s3", "uri": "s3://evil-bucket/file.json"},
                    "include_provenance": False,
                }, None)
        assert "error" in result or (
            isinstance(result.get("body"), str) and
            "allowlist" in result["body"]
        )


# ---------------------------------------------------------------------------
# #66 — Plan templating
# ---------------------------------------------------------------------------

class TestPlanTemplateResolution:
    """_resolve_template correctly substitutes {{var}} placeholders."""

    def _resolve(self, objective: str, values: dict):
        from tools.instantiate_plan.handler import _resolve_template
        return _resolve_template(objective, values)

    def test_single_variable_resolved(self):
        resolved, err = self._resolve("Find {{disease}} patients", {"disease": "diabetes"})
        assert err is None
        assert resolved == "Find diabetes patients"

    def test_multiple_variables_resolved(self):
        resolved, err = self._resolve(
            "Find {{disease}} with {{test}} since {{date}}",
            {"disease": "cancer", "test": "CBC", "date": "2024-01"},
        )
        assert err is None
        assert "cancer" in resolved
        assert "CBC" in resolved
        assert "2024-01" in resolved

    def test_missing_variable_returns_error(self):
        _, err = self._resolve("Find {{disease}} patients", {})
        assert err is not None
        assert "disease" in err

    def test_extra_values_ignored(self):
        resolved, err = self._resolve("Find {{disease}} patients",
                                       {"disease": "asthma", "unused": "value"})
        assert err is None
        assert resolved == "Find asthma patients"

    def test_nested_template_injection_rejected(self):
        _, err = self._resolve("Find {{disease}} patients",
                               {"disease": "{{injected}}"})
        assert err is not None
        assert "nested" in err.lower() or "{{" in err

    def test_no_placeholders_passes_through(self):
        resolved, err = self._resolve("Find all patients with cancer", {})
        assert err is None
        assert resolved == "Find all patients with cancer"


class TestPlanHandlerTemplate:
    """plan handler stores template without invoking LLM when is_template=True."""

    def _call(self, extra_body: dict | None = None):
        import importlib
        import tools.plan.handler as plan_mod
        importlib.reload(plan_mod)

        body = {
            "objective": "Find {{disease}} patients since {{start_date}}",
            "source_id": "athena:db.patients",
            "is_template": True,
            "template_variables": {"disease": "cancer", "start_date": "2024-01"},
        }
        if extra_body:
            body.update(extra_body)

        stored: dict = {}

        def fake_store(pid, plan):
            stored["plan_id"] = pid
            stored["plan"] = plan

        with patch.object(plan_mod, "store_plan", side_effect=fake_store), \
             patch.object(plan_mod, "audit_log"), \
             patch.object(plan_mod, "new_plan_id", return_value="plan-tmpl1234"):
            result = plan_mod.handler(body, None)

        return result, stored

    def test_returns_template_status(self):
        result, _ = self._call()
        assert result.get("status") == "template" or (
            isinstance(result.get("body"), str) and
            json.loads(result["body"]).get("status") == "template"
        )

    def test_stores_objective_in_plan(self):
        _, stored = self._call()
        assert "objective" in stored.get("plan", {})
        assert "{{disease}}" in stored["plan"]["objective"]

    def test_stores_template_variables(self):
        _, stored = self._call()
        assert stored.get("plan", {}).get("template_variables") == {
            "disease": "cancer", "start_date": "2024-01",
        }

    def test_stores_empty_query_and_query_type(self):
        _, stored = self._call()
        assert stored.get("plan", {}).get("query") == ""
        assert stored.get("plan", {}).get("query_type") == ""

    def test_stores_status_template(self):
        _, stored = self._call()
        assert stored.get("plan", {}).get("status") == "template"

    def test_no_llm_invoked(self):
        import importlib
        import tools.plan.handler as plan_mod
        importlib.reload(plan_mod)

        body = {
            "objective": "Find {{disease}} patients",
            "source_id": "athena:db.patients",
            "is_template": True,
        }
        with patch.object(plan_mod, "store_plan"), \
             patch.object(plan_mod, "audit_log"), \
             patch.object(plan_mod, "new_plan_id", return_value="plan-tmpl1234"), \
             patch.object(plan_mod, "bedrock_runtime") as mock_bedrock, \
             patch.object(plan_mod, "call_router") as mock_router:
            plan_mod.handler(body, None)
        mock_bedrock.assert_not_called()
        mock_router.assert_not_called()


class TestExcavateBlocksTemplatePlans:
    """excavate handler rejects plan with status='template'."""

    def test_template_plan_returns_template_status(self):
        import importlib
        import tools.excavate.handler as exc_mod
        importlib.reload(exc_mod)

        mock_plan = {
            "source_id": "athena:db.patients",
            "query": "",
            "query_type": "",
            "status": "template",
            "created_by": "user@example.com",
            "constraints": {},
        }
        with patch.object(exc_mod, "load_plan", return_value=mock_plan), \
             patch.object(exc_mod, "audit_log"), \
             patch.object(exc_mod, "validate_source_id"):
            result = exc_mod.handler({
                "plan_id": "plan-tmpl1234",
                "source_id": "athena:db.patients",
                "query": "SELECT 1",  # non-empty to pass early validation; status check fires first
                "query_type": "athena_sql",
            }, None)

        # Response may be wrapped or direct dict
        body = result
        if isinstance(result.get("body"), str):
            body = json.loads(result["body"])
        assert body.get("status") == "template"
        assert "instantiate_plan" in body.get("message", "")


class TestInstantiatePlan:
    """instantiate_plan handler resolves template and delegates to plan handler."""

    def _call_instantiate(self, template_plan: dict, values: dict,
                           plan_result: dict | None = None) -> dict:
        import importlib
        import tools.instantiate_plan.handler as ip_mod
        importlib.reload(ip_mod)

        if plan_result is None:
            plan_result = {
                "plan_id": "plan-concrete01",
                "status": "ready",
                "steps": [],
                "estimated_cost": "$0.00",
            }

        with patch.object(ip_mod, "load_plan", return_value=template_plan), \
             patch.object(ip_mod, "audit_log"), \
             patch("tools.plan.handler.store_plan"), \
             patch("tools.plan.handler.audit_log"), \
             patch("tools.plan.handler.new_plan_id", return_value="plan-concrete01"), \
             patch("tools.plan.handler.get_cached_schema", return_value={
                 "columns": [{"name": "id", "type": "string"}]
             }), \
             patch("tools.plan.handler.call_router", return_value=None), \
             patch("tools.plan.handler.bedrock_runtime") as mock_br, \
             patch("tools.plan.handler.validate_source_id"):
            mock_br.return_value.invoke_model.return_value = {
                "body": __import__("io").BytesIO(json.dumps({
                    "content": [{"type": "text", "text": json.dumps({
                        "query": "SELECT * FROM patients WHERE disease = 'diabetes'",
                        "output_schema": {"columns": ["id"], "estimated_rows": 100},
                        "reasoning": "Simple filter query",
                    })}]
                }).encode())
            }
            result = ip_mod.handler({
                "plan_id": "plan-tmpl1234",
                "values": values,
                "requestContext": {
                    "authorizer": {"principalId": "user@example.com", "roles": "[]"},
                    "requestId": "req-001",
                },
            }, None)
        return result

    def _make_template(self, objective: str = "Find {{disease}} patients") -> dict:
        return {
            "plan_id": "plan-tmpl1234",
            "source_id": "athena:db.patients",
            "objective": objective,
            "query": "",
            "query_type": "",
            "status": "template",
            "created_by": "admin@example.com",
            "constraints": {"read_only": True, "timeout_seconds": 30},
            "template_variables": {"disease": "cancer"},
        }

    def test_rejects_non_template_plan(self):
        import importlib
        import tools.instantiate_plan.handler as ip_mod
        importlib.reload(ip_mod)
        non_template = {**self._make_template(), "status": "ready", "query": "SELECT 1"}
        with patch.object(ip_mod, "load_plan", return_value=non_template):
            result = ip_mod.handler({"plan_id": "plan-ready01", "values": {}}, None)
        assert "error" in result or (
            isinstance(result.get("body"), str) and
            "error" in json.loads(result["body"])
        )

    def test_rejects_missing_plan(self):
        import importlib
        import tools.instantiate_plan.handler as ip_mod
        importlib.reload(ip_mod)
        with patch.object(ip_mod, "load_plan", return_value=None):
            result = ip_mod.handler({"plan_id": "plan-missing", "values": {}}, None)
        assert "error" in result or (
            isinstance(result.get("body"), str) and
            "error" in json.loads(result["body"])
        )

    def test_rejects_missing_plan_id(self):
        import importlib
        import tools.instantiate_plan.handler as ip_mod
        importlib.reload(ip_mod)
        result = ip_mod.handler({"values": {}}, None)
        assert "error" in result or (
            isinstance(result.get("body"), str) and
            "error" in json.loads(result["body"])
        )

    def test_rejects_missing_variable(self):
        import importlib
        import tools.instantiate_plan.handler as ip_mod
        importlib.reload(ip_mod)
        template = self._make_template("Find {{disease}} and {{lab_test}} patients")
        with patch.object(ip_mod, "load_plan", return_value=template), \
             patch.object(ip_mod, "audit_log"):
            result = ip_mod.handler({
                "plan_id": "plan-tmpl1234",
                "values": {"disease": "cancer"},  # missing lab_test
            }, None)
        assert "error" in result or (
            isinstance(result.get("body"), str) and
            "error" in json.loads(result["body"])
        )

    def test_rejects_nested_template_injection(self):
        import importlib
        import tools.instantiate_plan.handler as ip_mod
        importlib.reload(ip_mod)
        template = self._make_template("Find {{disease}} patients")
        with patch.object(ip_mod, "load_plan", return_value=template), \
             patch.object(ip_mod, "audit_log"):
            result = ip_mod.handler({
                "plan_id": "plan-tmpl1234",
                "values": {"disease": "{{injected}}"},
            }, None)
        assert "error" in result or (
            isinstance(result.get("body"), str) and
            "error" in json.loads(result["body"])
        )

    def test_successful_instantiation_calls_plan_handler(self):
        result = self._call_instantiate(
            self._make_template("Find {{disease}} patients"),
            {"disease": "diabetes"},
        )
        # Should not be an error response
        if isinstance(result.get("body"), str):
            body = json.loads(result["body"])
        else:
            body = result
        assert "error" not in body or body.get("plan_id") is not None
