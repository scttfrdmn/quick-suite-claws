"""
E2E tests for claws-plan.

Generates a concrete Athena SQL query from a natural-language objective
using Bedrock. The plan_result session fixture runs plan once; all tests
in this class reuse it.
"""

import pytest
from tools.tests.e2e.conftest import invoke, _E2E_SOURCE_ID, _E2E_TEAM_ID

pytestmark = pytest.mark.e2e


class TestPlanE2E:
    def test_plan_missing_objective_returns_error(self, lam, glue_table):
        """plan without objective returns an error."""
        result = invoke(lam, "claws-plan", {
            "source_id": _E2E_SOURCE_ID,
        })
        assert "error" in result, f"Expected error for missing objective: {result}"

    def test_plan_missing_source_id_returns_error(self, lam):
        """plan without source_id returns an error."""
        result = invoke(lam, "claws-plan", {
            "objective": "List all rows",
        })
        assert "error" in result, f"Expected error for missing source_id: {result}"

    def test_plan_without_cached_schema_returns_error(self, lam):
        """plan with a source_id that was never probed returns an error."""
        result = invoke(lam, "claws-plan", {
            "source_id": "athena:claws_e2e.unprobed_table_xyz",
            "objective": "Count all rows",
        })
        assert "error" in result, f"Expected error for uncached schema: {result}"

    def test_plan_returns_plan_id(self, lam, plan_result):
        """plan returns a plan_id string."""
        assert plan_result.get("plan_id"), f"Missing plan_id: {plan_result}"
        assert isinstance(plan_result["plan_id"], str)

    def test_plan_id_format(self, lam, plan_result):
        """plan_id starts with 'plan-'."""
        assert plan_result["plan_id"].startswith("plan-"), \
            f"Unexpected plan_id format: {plan_result['plan_id']}"

    def test_plan_returns_ready_status(self, lam, plan_result):
        """plan returns status=ready for a standard objective (no IRB)."""
        assert plan_result.get("status") == "ready", \
            f"Expected status=ready: {plan_result.get('status')}"

    def test_plan_returns_steps(self, lam, plan_result):
        """plan returns a steps list with at least one excavate step."""
        steps = plan_result.get("steps", [])
        assert len(steps) >= 1, f"Expected at least one step: {steps}"
        assert steps[0].get("tool") == "claws.excavate", \
            f"Expected excavate step: {steps[0]}"

    def test_plan_step_has_query(self, lam, plan_result):
        """The excavate step includes a non-empty query."""
        query = plan_result["steps"][0]["input"].get("query", "")
        assert query, f"Excavate step has empty query: {plan_result['steps'][0]}"

    def test_plan_step_query_is_read_only(self, lam, plan_result):
        """The generated query does not contain mutation keywords."""
        query = plan_result["steps"][0]["input"].get("query", "").upper()
        for bad_word in ("INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE"):
            assert bad_word not in query, \
                f"Query contains mutation keyword {bad_word}: {query}"

    def test_plan_returns_estimated_cost(self, lam, plan_result):
        """plan returns an estimated_cost string."""
        assert plan_result.get("estimated_cost"), f"Missing estimated_cost: {plan_result}"

    def test_plan_with_irb_returns_pending_status(self, lam, probe_result):
        """plan with requires_irb=true returns status=pending_approval."""
        result = invoke(lam, "claws-plan", {
            "source_id": _E2E_SOURCE_ID,
            "objective": "List all rows with their values.",
            "requires_irb": True,
        })
        if "error" in result:
            pytest.skip(f"plan call failed (Bedrock unavailable?): {result}")
        if result.get("status") == "blocked":
            pytest.skip(f"plan blocked by guardrail: {result}")
        assert result.get("status") == "pending_approval", \
            f"Expected pending_approval for IRB plan: {result.get('status')}"
        assert result.get("plan_id"), f"Missing plan_id in IRB plan: {result}"

    def test_plan_is_stored_in_dynamodb(self, lam, plan_result, plans_table):
        """plan writes the plan to the claws-plans DynamoDB table."""
        plan_id = plan_result["plan_id"]
        resp = plans_table.get_item(Key={"plan_id": plan_id})
        item = resp.get("Item")
        assert item is not None, f"Plan {plan_id} not found in DynamoDB"
        assert item.get("status") == "ready", f"Unexpected status in DDB: {item.get('status')}"

    def test_plan_with_team_id_stores_team(self, lam, plan_result, plans_table):
        """plan with team_id stores it in DynamoDB."""
        plan_id = plan_result["plan_id"]
        resp = plans_table.get_item(Key={"plan_id": plan_id})
        item = resp.get("Item")
        assert item.get("team_id") == _E2E_TEAM_ID, \
            f"team_id not stored: {item.get('team_id')}"
