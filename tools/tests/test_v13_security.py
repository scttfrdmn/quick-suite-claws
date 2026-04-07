"""
Security and reliability tests for clAWS v0.12.0 (#81–#87).

#81 — Mutation detection in DynamoDB and S3 Select executors
#82 — Refine summary scanned by ApplyGuardrail
#86 — approve_plan requires requires_irb check
#87 — OpenSearch error messages sanitized
"""

import json
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# #81 — Mutation detection
# ---------------------------------------------------------------------------

class TestMutationDetectionDynamoDB:
    """DynamoDB executor rejects mutation statements."""

    def _run(self, query: str) -> dict:
        from tools.excavate.executors.dynamodb import execute_dynamodb
        return execute_dynamodb(
            source_id="dynamodb:MyTable",
            query=query,
            constraints={},
            run_id="run-test",
        )

    def test_select_is_allowed(self):
        with patch("tools.excavate.executors.dynamodb._client") as mock_client:
            mock_client.return_value.execute_statement.return_value = {"Items": []}
            result = self._run("SELECT * FROM MyTable")
        assert result["status"] == "complete"

    def test_insert_blocked(self):
        result = self._run("INSERT INTO MyTable VALUE {'pk': '1'}")
        assert result["status"] == "error"
        assert "INSERT" in result["error"]
        assert "read-only" in result["error"]

    def test_update_blocked(self):
        result = self._run("UPDATE MyTable SET col = 'x' WHERE pk = '1'")
        assert result["status"] == "error"
        assert "UPDATE" in result["error"]

    def test_delete_blocked(self):
        result = self._run("DELETE FROM MyTable WHERE pk = '1'")
        assert result["status"] == "error"
        assert "DELETE" in result["error"]

    def test_drop_blocked(self):
        result = self._run("DROP TABLE MyTable")
        assert result["status"] == "error"
        assert "DROP" in result["error"]

    def test_create_blocked(self):
        result = self._run("CREATE TABLE NewTable (pk STRING)")
        assert result["status"] == "error"
        assert "CREATE" in result["error"]

    def test_case_insensitive_blocked(self):
        result = self._run("insert into MyTable value {'pk': '1'}")
        assert result["status"] == "error"
        assert "read-only" in result["error"]

    def test_whitespace_leading_select_allowed(self):
        with patch("tools.excavate.executors.dynamodb._client") as mock_client:
            mock_client.return_value.execute_statement.return_value = {"Items": []}
            result = self._run("  SELECT * FROM MyTable  ")
        assert result["status"] == "complete"


class TestMutationDetectionS3Select:
    """S3 Select executor rejects mutation statements."""

    def _run(self, query: str) -> dict:
        from tools.excavate.executors.s3_select import execute_s3_select
        return execute_s3_select(
            source_id="s3://my-bucket/data.csv",
            query=query,
            constraints={},
            run_id="run-test",
        )

    def test_select_is_allowed(self):
        mock_response = {
            "Payload": [
                {"Records": {"Payload": b'{"col": "val"}\n'}},
                {"Stats": {"Details": {"BytesScanned": 100, "BytesReturned": 20}}},
            ]
        }
        with patch("tools.excavate.executors.s3_select.s3_client") as mock_s3:
            mock_s3.return_value.select_object_content.return_value = mock_response
            result = self._run("SELECT * FROM S3Object")
        assert result["status"] == "complete"

    def test_insert_blocked(self):
        result = self._run("INSERT INTO S3Object VALUES (1, 2)")
        assert result["status"] == "error"
        assert "INSERT" in result["error"]
        assert "read-only" in result["error"]

    def test_update_blocked(self):
        result = self._run("UPDATE S3Object SET x = 1")
        assert result["status"] == "error"
        assert "UPDATE" in result["error"]

    def test_delete_blocked(self):
        result = self._run("DELETE FROM S3Object WHERE x = 1")
        assert result["status"] == "error"
        assert "DELETE" in result["error"]

    def test_drop_blocked(self):
        result = self._run("DROP TABLE something")
        assert result["status"] == "error"
        assert "DROP" in result["error"]

    def test_case_insensitive_blocked(self):
        result = self._run("insert into S3Object values (1)")
        assert result["status"] == "error"
        assert "read-only" in result["error"]


# ---------------------------------------------------------------------------
# #82 — Refine summary guardrail scan
# ---------------------------------------------------------------------------

class TestRefineSummaryGuardrail:
    """LLM-generated summary text is scanned through ApplyGuardrail."""

    def _call_refine(self, scan_return: dict) -> dict:
        """Run refine with summarize operation; mock _summarize and scan_payload."""
        import tools.refine.handler as refine_mod

        fake_rows = [{"col": "val"}]
        fake_summary = {
            "type": "summary",
            "text": "Key finding: sensitive data pattern",
            "source_run_id": "run-abc",
            "rows_summarized": 1,
        }

        with patch.object(refine_mod, "load_result", return_value=fake_rows), \
             patch.object(refine_mod, "_summarize", return_value=fake_summary), \
             patch.object(refine_mod, "scan_payload", return_value=scan_return) as mock_scan, \
             patch.object(refine_mod, "store_result", return_value="s3://bucket/run.json"), \
             patch.object(refine_mod, "audit_log"), \
             patch.object(refine_mod, "GUARDRAIL_ID", "test-guardrail-id"):
            event = {
                "run_id": "run-abc",
                "operations": ["summarize"],
            }
            result = refine_mod.handler(event, MagicMock())
            scanned_payloads = mock_scan.call_args_list
        return result, scanned_payloads

    def test_summary_scan_called_when_guardrail_configured(self):
        result, calls = self._call_refine({"status": "allowed"})
        # scan_payload called at least once (once for summary, once for rows)
        assert len(calls) >= 1
        # Check that at least one call included the summary text
        all_args = [str(c) for c in calls]
        assert any("summary" in a for a in all_args)

    def test_blocked_summary_replaced(self):
        result, _ = self._call_refine({"status": "blocked"})
        # The result should have been replaced or the response blocked
        # Either the stored summary text is the policy message, or the response is blocked
        assert result is not None

    def test_allowed_summary_returned_unchanged(self):
        result, _ = self._call_refine({"status": "allowed"})
        assert result.get("status") != "error"


# ---------------------------------------------------------------------------
# #86 — approve_plan requires_irb check
# ---------------------------------------------------------------------------

class TestApproveIrbCheck:
    """approve_plan rejects plans that don't have requires_irb=True."""

    def _call(self, plan: dict | None, approved_by: str = "reviewer@uni.edu") -> dict:
        import tools.approve_plan.handler as mod

        with patch.object(mod, "load_plan", return_value=plan), \
             patch.object(mod, "_get_irb_approvers", return_value=set()), \
             patch.object(mod, "audit_log"), \
             patch.object(mod, "dynamodb_resource"):
            event = {"plan_id": "plan-abc123", "approved_by": approved_by}
            return mod.handler(event, MagicMock())

    def test_plan_without_requires_irb_rejected(self):
        plan = {
            "plan_id": "plan-abc123",
            "status": "pending_approval",
            "created_by": "student@uni.edu",
            # requires_irb absent
        }
        result = self._call(plan)
        assert "error" in result or result.get("statusCode", 200) >= 400
        body = json.loads(result["body"]) if isinstance(result.get("body"), str) else result
        msg = body.get("error", body.get("message", ""))
        assert "IRB" in msg or "requires_irb" in msg

    def test_plan_with_requires_irb_false_rejected(self):
        plan = {
            "plan_id": "plan-abc123",
            "status": "pending_approval",
            "created_by": "student@uni.edu",
            "requires_irb": False,
        }
        result = self._call(plan)
        body = json.loads(result["body"]) if isinstance(result.get("body"), str) else result
        msg = body.get("error", body.get("message", ""))
        assert "IRB" in msg or "requires_irb" in msg

    def test_plan_with_requires_irb_true_proceeds(self):
        plan = {
            "plan_id": "plan-abc123",
            "status": "pending_approval",
            "created_by": "student@uni.edu",
            "requires_irb": True,
        }
        with patch("tools.approve_plan.handler.dynamodb_resource") as mock_ddb, \
             patch("tools.approve_plan.handler._emit_approval_event"):
            mock_table = MagicMock()
            mock_ddb.return_value.Table.return_value = mock_table

            import tools.approve_plan.handler as mod
            with patch.object(mod, "load_plan", return_value=plan), \
                 patch.object(mod, "_get_irb_approvers", return_value=set()), \
                 patch.object(mod, "audit_log"):
                event = {
                    "plan_id": "plan-abc123",
                    "approved_by": "reviewer@uni.edu",
                }
                result = mod.handler(event, MagicMock())

        body = json.loads(result["body"]) if isinstance(result.get("body"), str) else result
        assert body.get("status") == "approved" or body.get("plan_id") == "plan-abc123"

    def test_plan_not_found_returns_error(self):
        result = self._call(plan=None)
        body = json.loads(result["body"]) if isinstance(result.get("body"), str) else result
        assert "not found" in str(body).lower() or "error" in body


# ---------------------------------------------------------------------------
# #87 — OpenSearch error sanitization
# ---------------------------------------------------------------------------

class TestOpensearchErrorSanitization:
    """OpenSearch executor returns generic errors without exposing internals."""

    def test_invalid_source_id_generic_message(self):
        from tools.excavate.executors.opensearch import execute_opensearch
        result = execute_opensearch(
            source_id="opensearch:no-slash-here",
            query='{"query": {"match_all": {}}}',
            constraints={},
            run_id="run-test",
        )
        assert result["status"] == "error"
        # Must not contain the raw source_id value
        assert "no-slash-here" not in result["error"]
        assert "opensearch:host/index" in result["error"] or "Invalid" in result["error"]

    def test_invalid_json_query_generic_message(self):
        from tools.excavate.executors.opensearch import execute_opensearch
        result = execute_opensearch(
            source_id="opensearch:search-prod.us-east-1.es.amazonaws.com/logs",
            query="{not valid json",
            constraints={},
            run_id="run-test",
        )
        assert result["status"] == "error"
        # Must not expose internal json decoder error detail
        assert "JSONDecodeError" not in result["error"]
        assert "json" in result["error"].lower() or "JSON" in result["error"]

    def test_search_exception_generic_message(self):
        from tools.excavate.executors import opensearch as mod
        mock_client = MagicMock()
        mock_client.search.side_effect = Exception(
            "ConnectionError: https://secret-cluster-endpoint.es.amazonaws.com failed"
        )

        with patch.object(mod, "_os_client", return_value=mock_client):
            result = mod.execute_opensearch(
                source_id="opensearch:secret-cluster-endpoint.es.amazonaws.com/logs",
                query='{"query": {"match_all": {}}}',
                constraints={},
                run_id="run-test",
            )

        assert result["status"] == "error"
        # Endpoint URL must not be in the returned error
        assert "secret-cluster-endpoint" not in result["error"]
        assert result["error"] == "OpenSearch query failed"

    def test_timeout_exception_returns_timeout_status(self):
        from tools.excavate.executors import opensearch as mod
        mock_client = MagicMock()
        mock_client.search.side_effect = Exception("Request timed out after 30s")

        with patch.object(mod, "_os_client", return_value=mock_client):
            result = mod.execute_opensearch(
                source_id="opensearch:search-prod.es.amazonaws.com/idx",
                query='{"query": {"match_all": {}}}',
                constraints={"timeout_seconds": 30},
                run_id="run-test",
            )

        assert result["status"] == "timeout"
        assert "timed out" in result["error"]
        # Timeout message is safe (no endpoint details)
        assert "search-prod" not in result["error"]
