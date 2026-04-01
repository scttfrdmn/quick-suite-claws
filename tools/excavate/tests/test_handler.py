"""Handler-level tests for claws.excavate using moto S3 + DynamoDB mocks."""

import json
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

import tools.shared as _shared
from tools.excavate.handler import handler
from tools.shared import store_plan


@pytest.fixture(autouse=True)
def aws_credentials(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("CLAWS_RUNS_BUCKET", "claws-runs-test")
    monkeypatch.setenv("CLAWS_PLANS_TABLE", "claws-plans")
    monkeypatch.setenv("CLAWS_GUARDRAIL_ID", "")


@pytest.fixture(autouse=True)
def reset_clients():
    _shared._s3 = None
    _shared._dynamodb = None
    yield
    _shared._s3 = None
    _shared._dynamodb = None


@pytest.fixture
def aws_resources():
    """Create S3 bucket and DynamoDB plan table in moto."""
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="claws-runs-test")

    ddb = boto3.resource("dynamodb", region_name="us-east-1")
    ddb.create_table(
        TableName="claws-plans",
        KeySchema=[{"AttributeName": "plan_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "plan_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    return s3, ddb


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

    @mock_aws
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

    @mock_aws
    def test_plan_mismatch_returns_403(self, aws_resources):
        s3, ddb = aws_resources
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

    @mock_aws
    def test_athena_complete(self, aws_resources):
        """Patch execute_athena to avoid real Athena calls."""
        mock_result = {
            "status": "complete",
            "rows": [{"gene": "BRCA1", "chromosome": "17"}],
            "bytes_scanned": 1024,
            "cost": "$0.0000",
        }

        with patch("tools.excavate.handler.execute_athena", return_value=mock_result):
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
        run_id = body["run_id"]
        obj = s3.get_object(Bucket="claws-runs-test", Key=f"{run_id}/result.json")
        stored = json.loads(obj["Body"].read())
        assert stored[0]["gene"] == "BRCA1"

    @mock_aws
    def test_s3_select_complete(self, aws_resources):
        """End-to-end S3 Select with a real small CSV in moto S3."""
        s3 = boto3.client("s3", region_name="us-east-1")

        # Upload a small CSV to the runs bucket (reused as source)
        csv_content = "gene,chromosome,position\nBRCA1,17,43044295\nTP53,17,7668402\nAPOE,19,44905791\n"
        s3.put_object(Bucket="claws-runs-test", Key="test/variants.csv", Body=csv_content.encode())

        resp = handler(
            {
                "source_id": "s3://claws-runs-test/test/variants.csv",
                "query": "SELECT * FROM S3Object",
                "query_type": "s3_select_sql",
            },
            None,
        )

        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["status"] == "complete"
        assert body["rows_returned"] == 3
