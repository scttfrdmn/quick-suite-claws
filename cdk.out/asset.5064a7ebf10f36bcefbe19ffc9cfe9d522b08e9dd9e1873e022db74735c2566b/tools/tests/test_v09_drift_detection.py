"""v0.9.0 Drift Detection tests — issues #45–#48.

Covers:
- diff_results utility (issue #45)
- drift condition type in watch runner (issue #46)
- diff_summary passthrough in export provenance (issue #47)
"""

import json
import time

import boto3
import pytest

from tools.export.handler import handler as export_handler
from tools.shared import diff_results, new_run_id, store_plan, store_result, store_watch
from tools.watch.runner import handler as runner_handler
from unittest.mock import patch


# ---------------------------------------------------------------------------
# Issue #45 — diff_results utility
# ---------------------------------------------------------------------------

class TestDiffResults:
    def _put_result(self, s3, bucket: str, key: str, rows: list[dict]) -> str:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(rows),
            ContentType="application/json",
        )
        return f"s3://{bucket}/{key}"

    def test_diff_added_rows(self, s3_bucket):
        """Rows in B but not A are counted as added."""
        s3 = boto3.client("s3", region_name="us-east-1")
        uri_a = self._put_result(s3, "claws-runs", "diff/a1.json",
                                 [{"id": "1", "v": "x"}, {"id": "2", "v": "y"}])
        uri_b = self._put_result(s3, "claws-runs", "diff/b1.json",
                                 [{"id": "1", "v": "x"}, {"id": "2", "v": "y"}, {"id": "3", "v": "z"}])

        result = diff_results(uri_a, uri_b, "id")
        assert result["added_count"] == 1
        assert result["removed_count"] == 0
        assert result["changed_count"] == 0
        assert result["unchanged_count"] == 2

    def test_diff_removed_rows(self, s3_bucket):
        """Rows in A but not B are counted as removed."""
        s3 = boto3.client("s3", region_name="us-east-1")
        uri_a = self._put_result(s3, "claws-runs", "diff/a2.json",
                                 [{"id": "1"}, {"id": "2"}, {"id": "3"}])
        uri_b = self._put_result(s3, "claws-runs", "diff/b2.json",
                                 [{"id": "1"}, {"id": "2"}])

        result = diff_results(uri_a, uri_b, "id")
        assert result["removed_count"] == 1
        assert result["added_count"] == 0

    def test_diff_changed_rows(self, s3_bucket):
        """Rows present in both but with different values are changed."""
        s3 = boto3.client("s3", region_name="us-east-1")
        uri_a = self._put_result(s3, "claws-runs", "diff/a3.json",
                                 [{"id": "1", "val": "old"}, {"id": "2", "val": "same"}])
        uri_b = self._put_result(s3, "claws-runs", "diff/b3.json",
                                 [{"id": "1", "val": "new"}, {"id": "2", "val": "same"}])

        result = diff_results(uri_a, uri_b, "id")
        assert result["changed_count"] == 1
        assert result["unchanged_count"] == 1

    def test_diff_sample_rows_capped_at_5(self, s3_bucket):
        """Sample rows per category are capped at 5."""
        s3 = boto3.client("s3", region_name="us-east-1")
        rows_a = [{"id": str(i)} for i in range(10)]
        rows_b = [{"id": str(i + 10)} for i in range(10)]  # All new, none in common
        uri_a = self._put_result(s3, "claws-runs", "diff/a4.json", rows_a)
        uri_b = self._put_result(s3, "claws-runs", "diff/b4.json", rows_b)

        result = diff_results(uri_a, uri_b, "id")
        assert result["added_count"] == 10
        assert result["removed_count"] == 10
        assert len(result["added"]) <= 5
        assert len(result["removed"]) <= 5

    def test_diff_identical_datasets(self, s3_bucket):
        """Identical datasets → all unchanged, zero added/removed/changed."""
        s3 = boto3.client("s3", region_name="us-east-1")
        rows = [{"id": "1", "val": "a"}, {"id": "2", "val": "b"}]
        uri_a = self._put_result(s3, "claws-runs", "diff/a5.json", rows)
        uri_b = self._put_result(s3, "claws-runs", "diff/b5.json", rows)

        result = diff_results(uri_a, uri_b, "id")
        assert result["added_count"] == 0
        assert result["removed_count"] == 0
        assert result["changed_count"] == 0
        assert result["unchanged_count"] == 2


# ---------------------------------------------------------------------------
# Issue #46 — drift condition type in watch runner
# ---------------------------------------------------------------------------

class TestDriftCondition:
    def _seed_drift_watch(self, watch_id: str, plan_id: str, last_run_id: str | None,
                          threshold_pct: int = 10):
        store_plan(plan_id, {
            "source_id": "athena:db.t",
            "query": "SELECT id, val FROM t",
            "query_type": "athena_sql",
            "constraints": {},
        })
        spec: dict = {
            "plan_id": plan_id,
            "source_id": "athena:db.t",
            "schedule": "rate(1 day)",
            "type": "alert",
            "status": "active",
            "condition": {
                "type": "drift",
                "key_column": "id",
                "threshold_pct": threshold_pct,  # int — DynamoDB doesn't accept float
            },
            "ttl": int(time.time()) + 86400,
            "last_run_id": last_run_id,
            "last_run_at": None,
            "last_triggered_at": None,
            "consecutive_errors": 0,
        }
        store_watch(watch_id, spec)

    def test_drift_first_run_does_not_fire(self, s3_bucket, plans_table, watches_table):
        """First run of a drift watch (no prior baseline) never fires."""
        self._seed_drift_watch("watch-drift001", "plan-drift001", last_run_id=None)

        mock_result = {"status": "complete",
                       "rows": [{"id": "1"}, {"id": "2"}], "bytes_scanned": 0, "cost": "$0.00"}
        with patch.dict("tools.watch.runner.EXECUTORS",
                        {"athena_sql": lambda **kw: mock_result}):
            result = runner_handler({"watch_id": "watch-drift001"}, None)

        assert result["status"] == "complete"
        assert result["triggered"] is False

    def test_drift_fires_when_change_exceeds_threshold(self, s3_bucket, plans_table, watches_table):
        """Drift fires when (added+removed+changed)/total > threshold_pct."""
        # Seed a previous result in S3 as if from a prior run
        s3 = boto3.client("s3", region_name="us-east-1")
        prev_run_id = "run-driftprev1"
        prev_rows = [{"id": str(i)} for i in range(10)]
        s3.put_object(Bucket="claws-runs", Key=f"{prev_run_id}/result.json",
                      Body=json.dumps(prev_rows))

        self._seed_drift_watch("watch-drift002", "plan-drift002",
                               last_run_id=prev_run_id, threshold_pct=10)

        # New result has 8 old rows + 2 new rows = 20% new → exceeds 10% threshold
        new_rows = [{"id": str(i)} for i in range(8)] + [{"id": "100"}, {"id": "101"}]
        mock_result = {"status": "complete", "rows": new_rows, "bytes_scanned": 0, "cost": "$0.00"}

        with patch.dict("tools.watch.runner.EXECUTORS",
                        {"athena_sql": lambda **kw: mock_result}):
            result = runner_handler({"watch_id": "watch-drift002"}, None)

        assert result["status"] == "complete"
        assert result["triggered"] is True
        assert "diff_summary" in result
        assert result["diff_summary"]["added_count"] > 0

    def test_drift_does_not_fire_below_threshold(self, s3_bucket, plans_table, watches_table):
        """Drift does not fire when change is below threshold_pct."""
        s3 = boto3.client("s3", region_name="us-east-1")
        prev_run_id = "run-driftprev2"
        prev_rows = [{"id": str(i)} for i in range(20)]
        s3.put_object(Bucket="claws-runs", Key=f"{prev_run_id}/result.json",
                      Body=json.dumps(prev_rows))

        self._seed_drift_watch("watch-drift003", "plan-drift003",
                               last_run_id=prev_run_id, threshold_pct=50)

        # Only 1 row changed out of 20 = 5% → below 50% threshold
        new_rows = list(prev_rows)
        new_rows[0] = {"id": "999"}  # replace one
        mock_result = {"status": "complete", "rows": new_rows, "bytes_scanned": 0, "cost": "$0.00"}

        with patch.dict("tools.watch.runner.EXECUTORS",
                        {"athena_sql": lambda **kw: mock_result}):
            result = runner_handler({"watch_id": "watch-drift003"}, None)

        assert result["status"] == "complete"
        assert result["triggered"] is False


# ---------------------------------------------------------------------------
# Issue #47 — diff_summary in export provenance
# ---------------------------------------------------------------------------

class TestExportDiffSummary:
    def test_diff_summary_included_in_provenance(self, s3_bucket):
        """Export with diff_summary passes it through to the provenance file."""
        run_id = new_run_id()
        store_result(run_id, [{"id": "1", "val": "x"}])

        sample_diff = {
            "added": [{"id": "99"}],
            "removed": [],
            "changed": [],
            "added_count": 1,
            "removed_count": 0,
            "changed_count": 0,
            "unchanged_count": 5,
        }

        resp = export_handler(
            {
                "run_id": run_id,
                "destination": {"type": "s3", "uri": "s3://claws-runs/drift-export.json"},
                "include_provenance": True,
                "diff_summary": sample_diff,
            },
            None,
        )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        prov_uri = body["provenance_uri"]

        # Verify provenance contains diff_summary
        prov_parts = prov_uri.replace("s3://", "").split("/", 1)
        s3 = boto3.client("s3", region_name="us-east-1")
        prov = json.loads(s3.get_object(Bucket=prov_parts[0], Key=prov_parts[1])["Body"].read())
        assert "diff_summary" in prov
        assert prov["diff_summary"]["added_count"] == 1

    def test_export_without_diff_summary_no_key(self, s3_bucket):
        """Export without diff_summary does not include the key in provenance."""
        run_id = new_run_id()
        store_result(run_id, [{"id": "2"}])

        resp = export_handler(
            {
                "run_id": run_id,
                "destination": {"type": "s3", "uri": "s3://claws-runs/no-drift.json"},
                "include_provenance": True,
            },
            None,
        )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        prov_uri = body["provenance_uri"]

        prov_parts = prov_uri.replace("s3://", "").split("/", 1)
        s3 = boto3.client("s3", region_name="us-east-1")
        prov = json.loads(s3.get_object(Bucket=prov_parts[0], Key=prov_parts[1])["Body"].read())
        assert "diff_summary" not in prov
