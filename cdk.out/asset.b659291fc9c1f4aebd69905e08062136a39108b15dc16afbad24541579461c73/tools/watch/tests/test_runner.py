"""Tests for the watch runner Lambda."""

import json
import time
from unittest.mock import MagicMock, patch

import boto3
import pytest

from tools.shared import load_watch, store_plan, store_watch
from tools.watch.runner import handler


@pytest.fixture()
def aws_resources(s3_bucket, plans_table, watches_table):
    return s3_bucket, plans_table, watches_table


def _seed(watch_id, plan_id, source_id, condition=None, status="active",
          notification_target=None):
    store_plan(plan_id, {
        "source_id": source_id,
        "query": "SELECT * FROM test",
        "query_type": "athena_sql",
        "constraints": {},
    })
    spec = {
        "plan_id": plan_id,
        "source_id": source_id,
        "schedule": "rate(1 day)",
        "type": "alert",
        "status": status,
        "ttl": int(time.time()) + 86400,
        "last_run_id": None,
        "last_run_at": None,
        "last_triggered_at": None,
        "consecutive_errors": 0,
        "notification_target": notification_target or {},
    }
    if condition:
        spec["condition"] = condition
    store_watch(watch_id, spec)


_MOCK_RESULT_10 = {"status": "complete", "rows": [{"n": 150}],
                   "bytes_scanned": 0, "cost": "$0.00"}
_MOCK_RESULT_LOW = {"status": "complete", "rows": [{"n": 50}],
                    "bytes_scanned": 0, "cost": "$0.00"}
_MOCK_EMPTY = {"status": "complete", "rows": [], "bytes_scanned": 0, "cost": "$0.00"}


class TestWatchRunner:
    def test_condition_gt_fires(self, aws_resources):
        _seed("watch-run00001", "plan-run00001", "athena:db.t",
              condition={"field": "n", "operator": "gt", "threshold": 100})

        with patch.dict("tools.watch.runner.EXECUTORS",
                        {"athena_sql": lambda **kw: _MOCK_RESULT_10}):
            result = handler({"watch_id": "watch-run00001"}, None)

        assert result["status"] == "complete"
        assert result["triggered"] is True

    def test_condition_gt_does_not_fire(self, aws_resources):
        _seed("watch-run00002", "plan-run00002", "athena:db.t",
              condition={"field": "n", "operator": "gt", "threshold": 100})

        with patch.dict("tools.watch.runner.EXECUTORS",
                        {"athena_sql": lambda **kw: _MOCK_RESULT_LOW}):
            result = handler({"watch_id": "watch-run00002"}, None)

        assert result["status"] == "complete"
        assert result["triggered"] is False

    def test_no_condition_always_fires(self, aws_resources):
        """Watch without condition always fires notification."""
        _seed("watch-run00003", "plan-run00003", "athena:db.t")

        with patch.dict("tools.watch.runner.EXECUTORS",
                        {"athena_sql": lambda **kw: _MOCK_EMPTY}):
            result = handler({"watch_id": "watch-run00003"}, None)

        assert result["triggered"] is True

    def test_audit_log_principal(self, aws_resources, capsys):
        _seed("watch-run00004", "plan-run00004", "athena:db.t")

        with patch.dict("tools.watch.runner.EXECUTORS",
                        {"athena_sql": lambda **kw: _MOCK_EMPTY}):
            handler({"watch_id": "watch-run00004"}, None)

        logs = capsys.readouterr().out
        record = json.loads(logs.strip().splitlines()[-1])
        assert record.get("principal") == "watch-scheduler"
        assert record["outputs"].get("watch_id") == "watch-run00004"

    def test_last_run_updated(self, aws_resources):
        _seed("watch-run00005", "plan-run00005", "athena:db.t")

        with patch.dict("tools.watch.runner.EXECUTORS",
                        {"athena_sql": lambda **kw: _MOCK_EMPTY}):
            result = handler({"watch_id": "watch-run00005"}, None)

        updated = load_watch("watch-run00005")
        assert updated["last_run_id"] == result["run_id"]
        assert updated["last_run_at"] is not None

    def test_runner_sets_errored_on_executor_failure(self, aws_resources):
        _seed("watch-run00006", "plan-run00006", "athena:db.t")

        def _fail(**kw):
            raise RuntimeError("executor exploded")

        with patch.dict("tools.watch.runner.EXECUTORS", {"athena_sql": _fail}):
            result = handler({"watch_id": "watch-run00006"}, None)

        assert result["status"] == "error"
        updated = load_watch("watch-run00006")
        assert updated["status"] == "errored"
        assert "executor exploded" in updated["error_detail"]
