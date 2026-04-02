"""Handler tests for claws.watches (list / filter)."""

import json
import time

import pytest

from tools.shared import store_plan, store_watch
from tools.watches.handler import handler


@pytest.fixture()
def aws_resources(plans_table, watches_table):
    return plans_table, watches_table


def _seed_watch(watch_id, plan_id, source_id, status="active"):
    store_watch(watch_id, {
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
    })


class TestWatchesList:
    def test_list_all_watches(self, aws_resources):
        _seed_watch("watch-list0001", "plan-aa", "athena:db.t1")
        _seed_watch("watch-list0002", "plan-bb", "athena:db.t2")

        resp = handler({}, None)

        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        ids = {w["watch_id"] for w in body["watches"]}
        assert "watch-list0001" in ids
        assert "watch-list0002" in ids

    def test_filter_by_status(self, aws_resources):
        _seed_watch("watch-status01", "plan-cc", "athena:db.t3", status="active")
        _seed_watch("watch-status02", "plan-dd", "athena:db.t4", status="paused")

        resp = handler({"status_filter": "active"}, None)

        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert all(w["status"] == "active" for w in body["watches"])
        ids = {w["watch_id"] for w in body["watches"]}
        assert "watch-status01" in ids
        assert "watch-status02" not in ids

    def test_filter_by_source_id(self, aws_resources):
        _seed_watch("watch-src00001", "plan-ee", "athena:oncology.variants")
        _seed_watch("watch-src00002", "plan-ff", "athena:finance.ledger")

        resp = handler({"source_id_filter": "athena:oncology.variants"}, None)

        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert len(body["watches"]) >= 1
        assert all(w["source_id"] == "athena:oncology.variants" for w in body["watches"])
