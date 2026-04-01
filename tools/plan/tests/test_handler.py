"""Handler-level tests for claws.plan using moto DynamoDB + mocked Bedrock."""

import json
from unittest.mock import MagicMock, patch

import boto3
import pytest
from moto import mock_aws

import tools.shared as _shared
from tools.plan.handler import handler
from tools.shared import load_plan


@pytest.fixture(autouse=True)
def aws_credentials(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("CLAWS_PLANS_TABLE", "claws-plans")
    monkeypatch.setenv("CLAWS_SCHEMAS_TABLE", "claws-schemas")
    monkeypatch.setenv("CLAWS_GUARDRAIL_ID", "")  # disable guardrail in tests


@pytest.fixture(autouse=True)
def reset_clients():
    _shared._dynamodb = None
    _shared._bedrock = None
    yield
    _shared._dynamodb = None
    _shared._bedrock = None


@pytest.fixture
def dynamo_tables():
    """Create claws-plans and claws-schemas DynamoDB tables in moto."""
    ddb = boto3.resource("dynamodb", region_name="us-east-1")
    for table_name, key in [("claws-plans", "plan_id"), ("claws-schemas", "source_id")]:
        ddb.create_table(
            TableName=table_name,
            KeySchema=[{"AttributeName": key, "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": key, "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
    return ddb


SAMPLE_SCHEMA = {
    "database": "genomics",
    "table": "variants",
    "columns": [{"name": "gene", "type": "string"}, {"name": "chromosome", "type": "string"}],
    "size_bytes_estimate": 1_000_000_000,  # 1 GB
}


def _seed_schema(ddb, source_id: str, schema: dict):
    ddb.Table("claws-schemas").put_item(
        Item={"source_id": source_id, "schema": schema}
    )


def _bedrock_mock(query: str) -> MagicMock:
    """Return a mock bedrock_runtime() whose invoke_model returns a valid plan JSON."""
    body_content = json.dumps({
        "content": [
            {
                "type": "text",
                "text": json.dumps({
                    "query": query,
                    "output_schema": {"columns": ["gene", "chromosome"], "estimated_rows": 100},
                    "reasoning": "Test plan",
                }),
            }
        ]
    }).encode()
    mock_body = MagicMock()
    mock_body.read.return_value = body_content

    mock_client = MagicMock()
    mock_client.invoke_model.return_value = {"body": mock_body}
    return mock_client


class TestPlanHandler:
    def test_requires_objective(self):
        resp = handler({"source_id": "athena:db.t"}, None)
        assert resp["statusCode"] == 400

    def test_requires_source_id(self):
        resp = handler({"objective": "find genes"}, None)
        assert resp["statusCode"] == 400

    @mock_aws
    def test_requires_cached_schema(self, dynamo_tables):
        """Without a prior probe, plan should fail with 422."""
        resp = handler({"objective": "find genes", "source_id": "athena:genomics.variants"}, None)
        assert resp["statusCode"] == 422

    @mock_aws
    def test_stores_plan_when_valid(self, dynamo_tables):
        _seed_schema(dynamo_tables, "athena:genomics.variants", SAMPLE_SCHEMA)
        bedrock_client = _bedrock_mock("SELECT gene, chromosome FROM genomics.variants LIMIT 100")

        with patch("tools.shared.bedrock_runtime", return_value=bedrock_client):
            resp = handler(
                {
                    "objective": "find all variant genes",
                    "source_id": "athena:genomics.variants",
                    "constraints": {"max_cost_dollars": 10.0},
                },
                None,
            )

        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["status"] == "ready"
        assert "plan_id" in body

        # Verify plan was stored in DynamoDB
        plan = load_plan(body["plan_id"])
        assert plan is not None
        assert plan["source_id"] == "athena:genomics.variants"

    @mock_aws
    def test_rejects_mutation_query(self, dynamo_tables):
        _seed_schema(dynamo_tables, "athena:genomics.variants", SAMPLE_SCHEMA)
        bedrock_client = _bedrock_mock("INSERT INTO genomics.variants SELECT * FROM genomics.variants")

        with patch("tools.shared.bedrock_runtime", return_value=bedrock_client):
            resp = handler(
                {"objective": "copy data", "source_id": "athena:genomics.variants"},
                None,
            )

        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["status"] == "rejected"
        assert "mutation" in body["reason"].lower() or "write" in body["reason"].lower()

    @mock_aws
    def test_rejects_excess_cost(self, dynamo_tables):
        huge_schema = {**SAMPLE_SCHEMA, "size_bytes_estimate": 100_000_000_000_000}  # 100 TB
        _seed_schema(dynamo_tables, "athena:genomics.variants", huge_schema)
        bedrock_client = _bedrock_mock("SELECT * FROM genomics.variants")

        with patch("tools.shared.bedrock_runtime", return_value=bedrock_client):
            resp = handler(
                {
                    "objective": "get all data",
                    "source_id": "athena:genomics.variants",
                    "constraints": {"max_cost_dollars": 0.001},
                },
                None,
            )

        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["status"] == "rejected"
        assert "cost" in body["reason"].lower()
