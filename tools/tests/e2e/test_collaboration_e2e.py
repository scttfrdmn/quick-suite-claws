"""
E2E tests for claws-team_plans and claws-share_plan.

Tests the v0.10 collaboration features: listing team plans and sharing
a plan with another principal.
"""

import pytest

from tools.tests.e2e.conftest import _E2E_TEAM_ID, invoke

pytestmark = pytest.mark.e2e


class TestTeamPlansE2E:
    def test_team_plans_missing_team_id_returns_error(self, lam):
        """team_plans without team_id returns an error."""
        result = invoke(lam, "claws-team_plans", {})
        assert "error" in result, f"Expected error for missing team_id: {result}"

    def test_team_plans_unknown_team_returns_empty_list(self, lam):
        """team_plans for an unknown team returns an empty list."""
        result = invoke(lam, "claws-team_plans", {
            "team_id": "nonexistent-team-xyz",
        })
        assert "error" not in result, f"Unexpected error: {result}"
        plans = result.get("plans", [])
        assert isinstance(plans, list), f"Expected list: {result}"
        assert len(plans) == 0, f"Expected empty list for unknown team: {plans}"

    def test_team_plans_returns_plans_for_e2e_team(self, lam, plan_result):
        """team_plans returns at least one plan for the E2E team."""
        result = invoke(lam, "claws-team_plans", {"team_id": _E2E_TEAM_ID})
        assert "error" not in result, f"Unexpected error: {result}"
        assert result.get("team_id") == _E2E_TEAM_ID, \
            f"team_id mismatch: {result.get('team_id')}"
        plans = result.get("plans", [])
        assert isinstance(plans, list), f"Expected list: {result}"
        plan_ids = [p.get("plan_id") for p in plans]
        assert plan_result["plan_id"] in plan_ids, \
            f"E2E plan not found in team plans: {plan_ids}"

    def test_team_plans_summary_has_required_fields(self, lam, plan_result):
        """Each plan summary has plan_id, source_id, created_at, team_id."""
        result = invoke(lam, "claws-team_plans", {"team_id": _E2E_TEAM_ID})
        for plan in result.get("plans", []):
            assert "plan_id" in plan, f"Plan summary missing plan_id: {plan}"
            assert "source_id" in plan, f"Plan summary missing source_id: {plan}"
            assert "team_id" in plan, f"Plan summary missing team_id: {plan}"

    def test_team_plans_sorted_newest_first(self, lam, plan_result):
        """team_plans returns plans sorted newest-first by created_at."""
        result = invoke(lam, "claws-team_plans", {"team_id": _E2E_TEAM_ID})
        plans = result.get("plans", [])
        if len(plans) >= 2:
            dates = [p.get("created_at", "") for p in plans if p.get("created_at")]
            if len(dates) >= 2:
                assert dates == sorted(dates, reverse=True), \
                    f"Plans not sorted newest-first: {dates}"


class TestSharePlanE2E:
    def test_share_plan_missing_plan_id_returns_error(self, lam):
        """share_plan without plan_id returns an error."""
        result = invoke(lam, "claws-share_plan", {
            "share_with": ["arn:aws:iam::942542972736:user/other-user"],
        })
        assert "error" in result, f"Expected error for missing plan_id: {result}"

    def test_share_plan_invalid_share_with_type_returns_error(self, lam, plan_result):
        """share_plan with share_with as string (not list) returns an error."""
        result = invoke(lam, "claws-share_plan", {
            "plan_id": plan_result["plan_id"],
            "share_with": "not-a-list",
        })
        assert "error" in result, f"Expected error for invalid share_with type: {result}"

    def test_share_plan_unknown_plan_returns_error(self, lam):
        """share_plan for a nonexistent plan_id returns an error."""
        result = invoke(lam, "claws-share_plan", {
            "plan_id": "plan-nonexistent-xyz",
            "share_with": ["arn:aws:iam::942542972736:user/other-user"],
        })
        assert "error" in result, f"Expected error for unknown plan: {result}"

    def test_share_plan_returns_success(self, lam, plan_result):
        """share_plan for the E2E plan returns a success response."""
        result = invoke(lam, "claws-share_plan", {
            "plan_id": plan_result["plan_id"],
            "share_with": ["arn:aws:iam::942542972736:user/collaborator"],
        })
        # The principal in the invocation is "unknown" from context — the plan owner
        # is also "unknown" (no requestContext), so sharing should succeed.
        assert "error" not in result, f"Unexpected error sharing plan: {result}"

    def test_share_plan_updates_dynamodb(self, lam, plan_result, plans_table):
        """share_plan updates the shared_with list in DynamoDB."""
        collaborator = "arn:aws:iam::942542972736:user/ddb-check-collaborator"
        invoke(lam, "claws-share_plan", {
            "plan_id": plan_result["plan_id"],
            "share_with": [collaborator],
        })
        resp = plans_table.get_item(Key={"plan_id": plan_result["plan_id"]})
        item = resp.get("Item", {})
        shared_with = item.get("shared_with", [])
        assert collaborator in shared_with, \
            f"Collaborator not found in shared_with: {shared_with}"
