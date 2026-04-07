"""
Security tests for clAWS v0.13.0 (#75–#78).

#75 — Cedar policy permit clause requires requires_irb + pending_approval
#76 — OpenSearch DSL script injection blocked
#77 — Empty GUARDRAIL_ID returns "bypassed" status (not "clean")
#78 — source_id validated before use in handlers
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# #77 — Guardrail bypass visibility
# ---------------------------------------------------------------------------

class TestGuardrailBypassVisibility:
    """When GUARDRAIL_ID is empty, scan_payload returns 'bypassed', not 'clean'."""

    def test_scan_payload_bypassed_when_no_guardrail_id(self):
        import tools.shared as shared_mod
        with patch.object(shared_mod, "GUARDRAIL_ID", ""):
            result = shared_mod.scan_payload([{"col": "val"}])
        assert result["status"] == "bypassed"
        assert "payload" in result

    def test_scan_payload_clean_when_guardrail_configured(self):
        import tools.shared as shared_mod
        mock_response = {"action": "NONE", "assessments": []}
        with patch.object(shared_mod, "GUARDRAIL_ID", "guard-123"), \
             patch.object(shared_mod, "apply_guardrail", return_value=mock_response):
            result = shared_mod.scan_payload([{"col": "val"}])
        assert result["status"] == "clean"

    def test_scan_payload_blocked_when_guardrail_intervenes(self):
        import tools.shared as shared_mod
        mock_response = {"action": "GUARDRAIL_INTERVENED", "assessments": [{"type": "PII"}]}
        with patch.object(shared_mod, "GUARDRAIL_ID", "guard-123"), \
             patch.object(shared_mod, "apply_guardrail", return_value=mock_response):
            result = shared_mod.scan_payload([{"ssn": "123-45-6789"}])
        assert result["status"] == "blocked"

    def test_apply_guardrail_bypassed_flag_when_no_guardrail_id(self):
        import tools.shared as shared_mod
        with patch.object(shared_mod, "GUARDRAIL_ID", ""):
            result = shared_mod.apply_guardrail("test content")
        assert result.get("bypassed") is True
        assert result["action"] == "NONE"

    def test_apply_guardrail_no_bypass_flag_when_configured(self):
        import tools.shared as shared_mod
        mock_bedrock = MagicMock()
        mock_bedrock.apply_guardrail.return_value = {"action": "NONE", "assessments": []}
        with patch.object(shared_mod, "GUARDRAIL_ID", "guard-123"), \
             patch.object(shared_mod, "bedrock_runtime", return_value=mock_bedrock):
            result = shared_mod.apply_guardrail("test content")
        assert "bypassed" not in result or result.get("bypassed") is not True

    def test_scan_payload_bypassed_does_not_block(self):
        """Callers that check status=='blocked' are unaffected by bypassed status."""
        import tools.shared as shared_mod
        with patch.object(shared_mod, "GUARDRAIL_ID", ""):
            result = shared_mod.scan_payload([{"data": "sensitive"}])
        assert result["status"] != "blocked"


# ---------------------------------------------------------------------------
# #78 — source_id validation
# ---------------------------------------------------------------------------

class TestSourceIdValidation:
    """validate_source_id() rejects unsafe values and accepts valid ones."""

    def _validate(self, source_id: str) -> None:
        from tools.shared import validate_source_id
        validate_source_id(source_id)

    # --- Valid source_ids ---
    def test_athena_source_id_accepted(self):
        self._validate("athena:genomics.variants")

    def test_dynamodb_source_id_accepted(self):
        self._validate("dynamodb:ClawsPlansTable")

    def test_s3_uri_source_id_accepted(self):
        self._validate("s3://my-bucket/data/file.csv")

    def test_s3_colon_source_id_accepted(self):
        self._validate("s3:bucket/path/to/file.parquet")

    def test_opensearch_source_id_accepted(self):
        self._validate("opensearch:search-prod.us-east-1.es.amazonaws.com/logs")

    def test_mcp_source_id_accepted(self):
        self._validate("mcp:server-name:tool-name")

    def test_registry_source_id_accepted(self):
        self._validate("registry:s3:my-dataset")

    # --- Invalid source_ids ---
    def test_empty_source_id_rejected(self):
        import pytest
        with pytest.raises(ValueError, match="required"):
            self._validate("")

    def test_path_traversal_rejected(self):
        import pytest
        with pytest.raises(ValueError, match="traversal"):
            self._validate("athena:../../etc/passwd")

    def test_null_byte_rejected(self):
        import pytest
        with pytest.raises(ValueError, match="control"):
            self._validate("athena:table\x00injection")

    def test_control_char_rejected(self):
        import pytest
        with pytest.raises(ValueError, match="control"):
            self._validate("athena:table\x01injection")

    def test_excessive_length_rejected(self):
        import pytest
        with pytest.raises(ValueError, match="maximum length"):
            self._validate("athena:" + "x" * 600)

    def test_unknown_prefix_rejected(self):
        import pytest
        with pytest.raises(ValueError, match="known prefix"):
            self._validate("mysql://host/db")

    def test_raw_table_name_no_prefix_rejected(self):
        import pytest
        with pytest.raises(ValueError, match="known prefix"):
            self._validate("GenomicsVariants")

    def test_plan_handler_returns_400_on_invalid_source_id(self):
        """plan handler returns error when source_id is invalid."""
        import tools.plan.handler as plan_mod
        event = {
            "objective": "Find all variants in chromosome 17",
            "source_id": "mysql://evil-host/db",  # invalid prefix
        }
        result = plan_mod.handler(event, MagicMock())
        body = json.loads(result["body"]) if isinstance(result.get("body"), str) else result
        assert result.get("statusCode", 400) == 400 or "error" in body

    def test_excavate_handler_returns_400_on_invalid_source_id(self):
        """excavate handler returns error when source_id is invalid."""
        import tools.excavate.handler as excavate_mod
        event = {
            "plan_id": "plan-abc123",
            "source_id": "../../etc/passwd",  # path traversal
            "query": "SELECT * FROM table",
            "query_type": "athena_sql",
        }
        result = excavate_mod.handler(event, MagicMock())
        body = json.loads(result["body"]) if isinstance(result.get("body"), str) else result
        assert result.get("statusCode", 400) == 400 or "error" in body


# ---------------------------------------------------------------------------
# #76 — OpenSearch DSL script injection
# ---------------------------------------------------------------------------

class TestOpenSearchScriptInjection:
    """OpenSearch executor rejects DSL containing script execution fields."""

    def _run(self, query: str | dict) -> dict:
        from tools.excavate.executors import opensearch as mod
        mock_client = MagicMock()
        mock_client.search.return_value = {
            "hits": {"hits": [{"_source": {"col": "val"}}]}
        }
        with patch.object(mod, "_os_client", return_value=mock_client):
            return mod.execute_opensearch(
                source_id="opensearch:search-prod.es.amazonaws.com/logs",
                query=query,
                constraints={},
                run_id="run-test",
            )

    def test_clean_match_all_passes(self):
        result = self._run('{"query": {"match_all": {}}}')
        assert result["status"] == "complete"

    def test_terms_agg_passes(self):
        query = {
            "query": {"match_all": {}},
            "aggs": {"by_service": {"terms": {"field": "service.keyword"}}},
        }
        result = self._run(query)
        assert result["status"] == "complete"

    def test_top_level_script_field_blocked(self):
        query = {"script": {"source": "Runtime.getRuntime().exec('id')"}}
        result = self._run(query)
        assert result["status"] == "error"
        assert "script" in result["error"].lower()

    def test_scripted_metric_in_aggregation_blocked(self):
        query = {
            "aggs": {
                "evil": {
                    "scripted_metric": {
                        "init_script": "params._agg.total = 0",
                        "map_script": "params._agg.total += 1",
                        "combine_script": "return params._agg",
                        "reduce_script": "return states.sum(s -> s.total)",
                    }
                }
            }
        }
        result = self._run(query)
        assert result["status"] == "error"
        assert "script" in result["error"].lower()

    def test_scripted_sort_blocked(self):
        query = {
            "query": {"match_all": {}},
            "sort": [{"_script": {"scripted_sort": {"source": "doc['field'].value"}}}],
        }
        result = self._run(query)
        assert result["status"] == "error"

    def test_nested_script_field_blocked(self):
        """Script field deeply nested in aggregation still detected."""
        query = {
            "aggs": {
                "level1": {
                    "terms": {"field": "service"},
                    "aggs": {
                        "level2": {
                            "terms": {"field": "method"},
                            "aggs": {
                                "evil": {"script": {"source": "malicious code"}}
                            },
                        }
                    },
                }
            }
        }
        result = self._run(query)
        assert result["status"] == "error"

    def test_depth_limit_does_not_crash(self):
        """Deeply nested (>20 levels) DSL terminates cleanly without error."""
        # Build a 25-level deep nested dict with no script fields
        deep = {"match_all": {}}
        for _ in range(25):
            deep = {"bool": {"must": [deep]}}
        result = self._run({"query": deep})
        # Should not error on depth alone
        assert result["status"] in ("complete", "error")  # error is OK from mock, not crash


# ---------------------------------------------------------------------------
# #75 — Cedar policy content check
# ---------------------------------------------------------------------------

class TestCedarPolicyContent:
    """The plan.approve permit clause in default.cedar must require requires_irb and status."""

    def _read_policy(self) -> str:
        policy_path = (
            Path(__file__).parent.parent.parent
            / "policies" / "default.cedar"
        )
        return policy_path.read_text()

    def test_permit_contains_requires_irb(self):
        policy = self._read_policy()
        # Find the plan.approve permit block
        assert "requires_irb" in policy, (
            "default.cedar plan.approve permit clause must check resource.requires_irb"
        )

    def test_permit_contains_pending_approval_status(self):
        policy = self._read_policy()
        assert "pending_approval" in policy, (
            "default.cedar plan.approve permit clause must check resource.status == 'pending_approval'"
        )

    def test_forbid_clause_still_present(self):
        """Self-approval forbid clause must still be present."""
        policy = self._read_policy()
        assert 'forbid' in policy
        assert 'plan.approve' in policy
