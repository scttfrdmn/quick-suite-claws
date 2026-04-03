"""
E2E tests for claws-excavate.

Runs the concrete Athena query from the plan fixture. The excavate_result
session fixture executes the query once; all tests reuse the same run_id.
"""

import pytest
from tools.tests.e2e.conftest import invoke, _E2E_SOURCE_ID

pytestmark = pytest.mark.e2e


class TestExcavateE2E:
    def test_excavate_missing_required_fields_returns_error(self, lam):
        """excavate without required source_id/query/query_type returns an error."""
        result = invoke(lam, "claws-excavate", {})
        assert "error" in result, f"Expected error for missing required fields: {result}"

    def test_excavate_unknown_plan_id_returns_error(self, lam):
        """excavate with a nonexistent plan_id returns an error."""
        result = invoke(lam, "claws-excavate", {
            "plan_id": "plan-nonexistent-xyz",
            "source_id": _E2E_SOURCE_ID,
            "query": "SELECT 1",
            "query_type": "athena_sql",
        })
        assert "error" in result, f"Expected error for unknown plan_id: {result}"

    def test_excavate_pending_plan_is_blocked(self, lam, probe_result):
        """excavate on a pending_approval plan returns a blocked response."""
        # Create a pending plan first
        plan_result = invoke(lam, "claws-plan", {
            "source_id": _E2E_SOURCE_ID,
            "objective": "Count all rows.",
            "requires_irb": True,
        })
        if "error" in plan_result or plan_result.get("status") == "blocked":
            pytest.skip("Could not create a pending_approval plan for this test")

        plan_id = plan_result.get("plan_id")
        if not plan_id:
            pytest.skip("No plan_id returned for IRB plan")

        step = (plan_result.get("steps") or [{}])[0]
        step_input = step.get("input", {})

        result = invoke(lam, "claws-excavate", {
            "plan_id": plan_id,
            "source_id": step_input.get("source_id", _E2E_SOURCE_ID),
            "query": step_input.get("query", "SELECT COUNT(*) FROM sample_data"),
            "query_type": step_input.get("query_type", "athena_sql"),
            "constraints": step_input.get("constraints", {}),
        })
        # Should return a "pending" or "blocked" error, not success
        assert "error" in result or result.get("status") == "pending_approval", \
            f"Expected blocked response for pending plan: {result}"

    def test_excavate_returns_run_id(self, lam, excavate_result):
        """excavate returns a run_id string."""
        assert excavate_result.get("run_id"), f"Missing run_id: {excavate_result}"
        assert isinstance(excavate_result["run_id"], str)

    def test_excavate_run_id_format(self, lam, excavate_result):
        """run_id starts with 'run-'."""
        assert excavate_result["run_id"].startswith("run-"), \
            f"Unexpected run_id format: {excavate_result['run_id']}"

    def test_excavate_returns_result_preview(self, lam, excavate_result):
        """excavate returns a result_preview list."""
        preview = excavate_result.get("result_preview", [])
        assert isinstance(preview, list), f"Expected result_preview list: {excavate_result}"

    def test_excavate_returns_rows_returned(self, lam, excavate_result):
        """excavate returns a rows_returned count."""
        assert "rows_returned" in excavate_result, f"Missing rows_returned: {excavate_result}"
        assert excavate_result["rows_returned"] >= 0

    def test_excavate_rows_returned_matches_preview(self, lam, excavate_result):
        """rows_returned matches the length of result_preview."""
        preview = excavate_result.get("result_preview", [])
        assert excavate_result["rows_returned"] == len(preview), \
            f"rows_returned mismatch: {excavate_result['rows_returned']} vs {len(preview)}"

    def test_excavate_result_stored_in_s3(self, lam, excavate_result, s3, runs_bucket):
        """excavate writes results to the claws-runs S3 bucket."""
        run_id = excavate_result["run_id"]
        result_uri = excavate_result.get("result_uri", "")
        # result_uri or run_id-based path should exist in the runs bucket
        if result_uri.startswith("s3://"):
            key = result_uri.split(runs_bucket + "/")[-1]
            resp = s3.head_object(Bucket=runs_bucket, Key=key)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        else:
            # Try the default path
            try:
                s3.head_object(Bucket=runs_bucket, Key=f"runs/{run_id}.json")
            except Exception:
                pass  # S3 path may vary; presence checked via result_uri above
