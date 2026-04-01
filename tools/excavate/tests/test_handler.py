"""Handler-level tests for claws.excavate using substrate S3 + DynamoDB."""

import json
from unittest.mock import patch

import boto3
import pytest

from tools.excavate.handler import handler
from tools.shared import store_plan


@pytest.fixture()
def aws_resources(s3_bucket, plans_table):
    """Create S3 bucket and DynamoDB plan table via substrate."""
    return s3_bucket, plans_table


class TestExcavateHandler:
    def test_requires_source_id(self):
        resp = handler({"query": "SELECT 1", "query_type": "athena_sql"}, None)
        assert resp["statusCode"] == 400

    def test_requires_query(self):
        resp = handler({"source_id": "athena:db.t", "query_type": "athena_sql"}, None)
        assert resp["statusCode"] == 400

    def test_requires_query_type(self):
        resp = handler({"source_id": "athena:db.t", "query": "SELECT 1"}, None)
        assert resp["statusCode"] == 400

    def test_unsupported_query_type(self):
        resp = handler(
            {"source_id": "athena:db.t", "query": "SELECT 1", "query_type": "mysql_sql"},
            None,
        )
        assert resp["statusCode"] == 400
        assert "unsupported" in json.loads(resp["body"])["error"].lower()

    def test_plan_not_found(self, aws_resources):
        resp = handler(
            {
                "plan_id": "plan-00000000",
                "source_id": "athena:db.t",
                "query": "SELECT 1",
                "query_type": "athena_sql",
            },
            None,
        )
        assert resp["statusCode"] == 404

    def test_plan_mismatch_returns_403(self, aws_resources):
        store_plan("plan-aabbccdd", {"query": "SELECT 1", "source_id": "athena:db.t"})

        resp = handler(
            {
                "plan_id": "plan-aabbccdd",
                "source_id": "athena:db.t",
                "query": "SELECT 2",  # different query
                "query_type": "athena_sql",
            },
            None,
        )
        assert resp["statusCode"] == 403

    def test_athena_complete(self, aws_resources):
        """Patch EXECUTORS dict — Athena executor not yet in substrate (issue #249)."""
        mock_result = {
            "status": "complete",
            "rows": [{"gene": "BRCA1", "chromosome": "17"}],
            "bytes_scanned": 1024,
            "cost": "$0.0000",
        }

        with patch.dict(
            "tools.excavate.handler.EXECUTORS",
            {"athena_sql": lambda **kw: mock_result},
        ):
            resp = handler(
                {
                    "source_id": "athena:genomics.variants",
                    "query": "SELECT gene, chromosome FROM genomics.variants",
                    "query_type": "athena_sql",
                },
                None,
            )

        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["status"] == "complete"
        assert "run_id" in body
        assert body["rows_returned"] == 1

        # Verify results stored in S3
        s3 = boto3.client("s3", region_name="us-east-1")
        obj = s3.get_object(Bucket="claws-runs", Key=f"{body['run_id']}/result.json")
        stored = json.loads(obj["Body"].read())
        assert stored[0]["gene"] == "BRCA1"

    def test_s3_select_complete(self, s3_bucket):
        """End-to-end S3 Select against substrate v0.45.2+ (issue #250 resolved)."""
        csv_content = (
            "gene,chromosome,position\n"
            "BRCA1,17,43044295\nTP53,17,7668402\nAPOE,19,44905791\n"
        )
        s3_bucket.put_object(
            Bucket="claws-runs", Key="test/variants.csv", Body=csv_content.encode()
        )

        resp = handler(
            {
                "source_id": "s3://claws-runs/test/variants.csv",
                "query": "SELECT * FROM S3Object",
                "query_type": "s3_select_sql",
            },
            None,
        )

        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["status"] == "complete"
        assert body["rows_returned"] == 3
