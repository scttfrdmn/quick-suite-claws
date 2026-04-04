"""Handler tests for claws.watch (create / update / delete)."""

import json
from unittest.mock import MagicMock

import pytest

from tools.shared import store_plan
from tools.watch.handler import handler


@pytest.fixture()
def aws_resources(plans_table, watches_table):
    return plans_table, watches_table


@pytest.fixture(autouse=True)
def mock_scheduler(monkeypatch):
    """EventBridge Scheduler is not in Substrate — mock the client."""
    import tools.watch.handler as _mod
    fake = MagicMock()
    monkeypatch.setattr(_mod, "_scheduler", fake)
    monkeypatch.setattr(_mod, "WATCH_RUNNER_ARN", "arn:aws:lambda:us-east-1:123:function:runner")
    monkeypatch.setattr(_mod, "WATCH_RUNNER_ROLE_ARN", "arn:aws:iam::123:role/scheduler")
    return fake


class TestWatchCreate:
    def test_create_watch_valid(self, aws_resources):
        store_plan("plan-aabb1122", {"source_id": "athena:db.t", "query": "SELECT 1",
                                     "query_type": "athena_sql"})

        resp = handler({
            "action": "create",
            "plan_id": "plan-aabb1122",
            "schedule": "rate(1 day)",
            "condition": {"field": "n", "operator": "gt", "threshold": 100},
            "notification_target": {"type": "s3", "uri": "s3://bucket/key"},
        }, None)

        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["status"] == "created"
        assert body["watch_id"].startswith("watch-")

    def test_create_watch_unknown_plan(self, aws_resources):
        resp = handler({
            "action": "create",
            "plan_id": "plan-00000000",
            "schedule": "rate(1 day)",
        }, None)

        assert resp["statusCode"] == 422

    def test_create_watch_no_condition(self, aws_resources):
        """Absent condition is accepted — unconditional watch always fires."""
        store_plan("plan-nocond01", {"source_id": "athena:db.t", "query": "SELECT 1",
                                     "query_type": "athena_sql"})

        resp = handler({
            "action": "create",
            "plan_id": "plan-nocond01",
            "schedule": "cron(0 6 * * ? *)",
        }, None)

        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert "watch_id" in body


class TestWatchUpdate:
    def test_update_watch(self, aws_resources):
        store_plan("plan-upd00001", {"source_id": "athena:db.t", "query": "SELECT 1",
                                     "query_type": "athena_sql"})
        create_resp = handler({
            "action": "create",
            "plan_id": "plan-upd00001",
            "schedule": "rate(1 day)",
        }, None)
        watch_id = json.loads(create_resp["body"])["watch_id"]

        resp = handler({"action": "update", "watch_id": watch_id,
                        "schedule": "rate(12 hours)"}, None)

        assert resp["statusCode"] == 200
        assert json.loads(resp["body"])["status"] == "updated"


class TestWatchDelete:
    def test_delete_watch(self, aws_resources):
        store_plan("plan-del00001", {"source_id": "athena:db.t", "query": "SELECT 1",
                                     "query_type": "athena_sql"})
        create_resp = handler({
            "action": "create",
            "plan_id": "plan-del00001",
            "schedule": "rate(1 day)",
        }, None)
        watch_id = json.loads(create_resp["body"])["watch_id"]

        resp = handler({"action": "delete", "watch_id": watch_id}, None)

        assert resp["statusCode"] == 200
        assert json.loads(resp["body"])["status"] == "deleted"

    def test_delete_unknown_watch(self, aws_resources):
        resp = handler({"action": "delete", "watch_id": "watch-00000000"}, None)
        assert resp["statusCode"] == 404
