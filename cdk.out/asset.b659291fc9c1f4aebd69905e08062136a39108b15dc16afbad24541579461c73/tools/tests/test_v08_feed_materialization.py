"""v0.8.0 Feed Materialization tests — issues #41–#44.

Covers:
- merge refine operation (issue #41)
- feed watch type: accumulates results across runs (issue #42)
- export append/overwrite mode (issue #43)
"""

import json
import time

import boto3
import pytest

from tools.export.handler import handler as export_handler
from tools.refine.handler import handler as refine_handler
from tools.shared import new_run_id, store_plan, store_result, store_watch
from tools.watch.runner import handler as runner_handler
from unittest.mock import patch


# ---------------------------------------------------------------------------
# Issue #41 — merge refine operation
# ---------------------------------------------------------------------------

class TestRefinerMergeMode:
    def test_merge_adds_new_rows(self, s3_bucket):
        """Merge mode appends new rows not in existing dataset."""
        # Seed the "existing" dataset directly into S3
        existing = [{"id": "1", "val": "a"}, {"id": "2", "val": "b"}]
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket = "claws-runs"
        existing_key = "feeds/watch-merge01/feed.json"
        s3.put_object(
            Bucket=bucket,
            Key=existing_key,
            Body=json.dumps(existing),
            ContentType="application/json",
        )
        existing_uri = f"s3://{bucket}/{existing_key}"

        # New excavation results: id=2 is a duplicate, id=3 is new
        run_id = new_run_id()
        new_rows = [{"id": "2", "val": "b"}, {"id": "3", "val": "c"}]
        store_result(run_id, new_rows)

        resp = refine_handler(
            {
                "mode": "merge",
                "run_id": run_id,
                "result_s3_uri": existing_uri,
                "dedup_key": "id",
            },
            None,
        )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["mode"] == "merge"
        assert body["added_count"] == 1     # id=3 added
        assert body["duplicate_count"] == 1  # id=2 skipped
        assert body["merged_count"] == 3     # 2 existing + 1 new

    def test_merge_no_duplicates_in_result(self, s3_bucket):
        """Merged result contains union without duplicates."""
        existing = [{"id": "a"}, {"id": "b"}]
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket = "claws-runs"
        feed_key = "feeds/watch-merge02/feed.json"
        s3.put_object(Bucket=bucket, Key=feed_key, Body=json.dumps(existing))
        feed_uri = f"s3://{bucket}/{feed_key}"

        run_id = new_run_id()
        store_result(run_id, [{"id": "a"}, {"id": "c"}])

        resp = refine_handler(
            {"mode": "merge", "run_id": run_id, "result_s3_uri": feed_uri, "dedup_key": "id"},
            None,
        )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["added_count"] + body["duplicate_count"] == 2  # 2 input rows
        assert body["merged_count"] == 3  # a, b, c

        # Verify the actual S3 content has no duplicates
        obj = s3.get_object(Bucket=bucket, Key=feed_key)
        merged = json.loads(obj["Body"].read())
        ids = [r["id"] for r in merged]
        assert len(ids) == len(set(ids))

    def test_merge_requires_run_id(self, s3_bucket):
        """merge mode without run_id returns 400."""
        resp = refine_handler(
            {"mode": "merge", "result_s3_uri": "s3://claws-runs/f.json", "dedup_key": "id"},
            None,
        )
        assert resp["statusCode"] == 400

    def test_merge_requires_result_s3_uri(self, s3_bucket):
        """merge mode without result_s3_uri returns 400."""
        run_id = new_run_id()
        store_result(run_id, [{"id": "x"}])
        resp = refine_handler(
            {"mode": "merge", "run_id": run_id, "dedup_key": "id"},
            None,
        )
        assert resp["statusCode"] == 400

    def test_merge_requires_dedup_key(self, s3_bucket):
        """merge mode without dedup_key returns 400."""
        run_id = new_run_id()
        store_result(run_id, [{"id": "x"}])
        resp = refine_handler(
            {"mode": "merge", "run_id": run_id, "result_s3_uri": "s3://claws-runs/f.json"},
            None,
        )
        assert resp["statusCode"] == 400


# ---------------------------------------------------------------------------
# Issue #42 — feed watch type
# ---------------------------------------------------------------------------

class TestFeedWatch:
    def _seed_feed_watch(self, watch_id: str, plan_id: str, source_id: str,
                         feed_result_uri: str | None = None):
        store_plan(plan_id, {
            "source_id": source_id,
            "query": "SELECT id, val FROM test",
            "query_type": "athena_sql",
            "constraints": {},
        })
        spec = {
            "plan_id": plan_id,
            "source_id": source_id,
            "schedule": "rate(1 hour)",
            "type": "feed",
            "feed_dedup_key": "id",
            "status": "active",
            "ttl": int(time.time()) + 86400,
            "last_run_id": None,
            "last_run_at": None,
            "last_triggered_at": None,
            "consecutive_errors": 0,
        }
        if feed_result_uri:
            spec["feed_result_uri"] = feed_result_uri
        store_watch(watch_id, spec)

    def test_feed_first_run_stores_result(self, s3_bucket, plans_table, watches_table):
        """First run of a feed watch stores results and sets feed_result_uri."""
        self._seed_feed_watch("watch-feed001", "plan-feed001", "athena:db.t")

        mock_result = {
            "status": "complete",
            "rows": [{"id": "1", "val": "a"}, {"id": "2", "val": "b"}],
            "bytes_scanned": 0,
            "cost": "$0.00",
        }
        with patch.dict("tools.watch.runner.EXECUTORS",
                        {"athena_sql": lambda **kw: mock_result}):
            result = runner_handler({"watch_id": "watch-feed001"}, None)

        assert result["status"] == "complete"

        # feed_result_uri should now be set on the watch
        from tools.shared import load_watch
        updated = load_watch("watch-feed001")
        assert updated.get("feed_result_uri") is not None
        assert "feeds/watch-feed001" in updated["feed_result_uri"]

    def test_feed_second_run_accumulates_new_rows(self, s3_bucket, plans_table, watches_table):
        """Second run of a feed watch adds new rows; duplicates not added twice."""
        # Set up: seed an initial feed dataset in S3
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket = "claws-runs"
        feed_key = "feeds/watch-feed002/feed.json"
        initial_rows = [{"id": "1", "val": "a"}, {"id": "2", "val": "b"}]
        s3.put_object(Bucket=bucket, Key=feed_key, Body=json.dumps(initial_rows))
        feed_uri = f"s3://{bucket}/{feed_key}"

        self._seed_feed_watch("watch-feed002", "plan-feed002", "athena:db.t",
                              feed_result_uri=feed_uri)

        # Second run: rows id=2 (duplicate) + id=3 (new)
        mock_result = {
            "status": "complete",
            "rows": [{"id": "2", "val": "b"}, {"id": "3", "val": "c"}],
            "bytes_scanned": 0,
            "cost": "$0.00",
        }
        with patch.dict("tools.watch.runner.EXECUTORS",
                        {"athena_sql": lambda **kw: mock_result}):
            result = runner_handler({"watch_id": "watch-feed002"}, None)

        assert result["status"] == "complete"

        # The feed file should now contain 3 rows: id=1, id=2, id=3
        obj = s3.get_object(Bucket=bucket, Key=feed_key)
        merged = json.loads(obj["Body"].read())
        ids = {r["id"] for r in merged}
        assert ids == {"1", "2", "3"}

    def test_non_feed_watch_replaces_result(self, s3_bucket, plans_table, watches_table):
        """Alert watches do not set feed_result_uri (non-accumulating)."""
        store_plan("plan-feed003", {
            "source_id": "athena:db.t",
            "query": "SELECT id FROM t",
            "query_type": "athena_sql",
            "constraints": {},
        })
        store_watch("watch-feed003", {
            "plan_id": "plan-feed003",
            "source_id": "athena:db.t",
            "schedule": "rate(1 day)",
            "type": "alert",
            "status": "active",
            "ttl": int(time.time()) + 86400,
            "last_run_id": None,
            "last_run_at": None,
            "last_triggered_at": None,
            "consecutive_errors": 0,
        })

        mock_result = {"status": "complete", "rows": [{"id": "x"}],
                       "bytes_scanned": 0, "cost": "$0.00"}
        with patch.dict("tools.watch.runner.EXECUTORS",
                        {"athena_sql": lambda **kw: mock_result}):
            result = runner_handler({"watch_id": "watch-feed003"}, None)

        assert result["status"] == "complete"
        from tools.shared import load_watch
        updated = load_watch("watch-feed003")
        assert updated.get("feed_result_uri") is None


# ---------------------------------------------------------------------------
# Issue #43 — export append/overwrite mode
# ---------------------------------------------------------------------------

class TestExportMode:
    def test_overwrite_replaces_object(self, s3_bucket):
        """overwrite mode writes to the exact specified key."""
        run_id = new_run_id()
        store_result(run_id, [{"id": "1"}])

        resp = export_handler(
            {
                "run_id": run_id,
                "destination": {"type": "s3", "uri": "s3://claws-runs/export-overwrite.json"},
                "mode": "overwrite",
            },
            None,
        )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["status"] == "complete"
        assert body["export_mode"] == "overwrite"
        assert body["destination_uri"] == "s3://claws-runs/export-overwrite.json"

        # Object must exist at the exact key
        s3 = boto3.client("s3", region_name="us-east-1")
        obj = s3.get_object(Bucket="claws-runs", Key="export-overwrite.json")
        assert json.loads(obj["Body"].read()) == [{"id": "1"}]

    def test_append_creates_new_timestamped_object(self, s3_bucket):
        """append mode creates a new object with a unique timestamped key."""
        run_id = new_run_id()
        store_result(run_id, [{"id": "2"}])

        resp = export_handler(
            {
                "run_id": run_id,
                "destination": {"type": "s3", "uri": "s3://claws-runs/export-base.json"},
                "mode": "append",
            },
            None,
        )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["status"] == "complete"
        assert body["export_mode"] == "append"
        # The actual destination_uri should be different from the base URI
        assert body["destination_uri"] != "s3://claws-runs/export-base.json"
        assert "export-base" in body["destination_uri"]

        # The base key should NOT exist (append creates a new one)
        s3 = boto3.client("s3", region_name="us-east-1")
        keys = [o["Key"] for o in s3.list_objects_v2(Bucket="claws-runs").get("Contents", [])]
        assert "export-base.json" not in keys

    def test_default_mode_is_overwrite(self, s3_bucket):
        """No mode specified → defaults to overwrite."""
        run_id = new_run_id()
        store_result(run_id, [{"id": "3"}])
        resp = export_handler(
            {
                "run_id": run_id,
                "destination": {"type": "s3", "uri": "s3://claws-runs/default-mode.json"},
            },
            None,
        )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["export_mode"] == "overwrite"

    def test_provenance_includes_export_mode(self, s3_bucket):
        """Provenance file contains export_mode field."""
        run_id = new_run_id()
        store_result(run_id, [{"id": "4"}])
        resp = export_handler(
            {
                "run_id": run_id,
                "destination": {"type": "s3", "uri": "s3://claws-runs/prov-mode.json"},
                "mode": "append",
                "include_provenance": True,
            },
            None,
        )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        prov_uri = body["provenance_uri"]

        # Load and verify provenance
        prov_parts = prov_uri.replace("s3://", "").split("/", 1)
        s3 = boto3.client("s3", region_name="us-east-1")
        prov = json.loads(s3.get_object(Bucket=prov_parts[0], Key=prov_parts[1])["Body"].read())
        assert prov["export_mode"] == "append"
