"""End-to-end pipeline tests: probe → plan → excavate.

Validates the core plan_id bait-and-switch protection across handler boundaries.
"""

import json
from unittest.mock import MagicMock, patch

import boto3
import pytest

from tools.excavate.handler import handler as excavate_handler
from tools.plan.handler import handler as plan_handler
from tools.shared import cache_schema

SAMPLE_QUERY = "SELECT gene, chromosome FROM genomics.variants LIMIT 10"
SAMPLE_SCHEMA = {
    "database": "genomics",
    "table": "variants",
    "columns": [{"name": "gene", "type": "string"}, {"name": "chromosome", "type": "string"}],
    "size_bytes_estimate": 1_000_000_000,
}


def _bedrock_mock(query: str) -> MagicMock:
    body_content = json.dumps({
        "content": [
            {
                "type": "text",
                "text": json.dumps({
                    "query": query,
                    "output_schema": {"columns": ["gene", "chromosome"], "estimated_rows": 10},
                    "reasoning": "Pipeline test plan",
                }),
            }
        ]
    }).encode()
    mock_body = MagicMock()
    mock_body.read.return_value = body_content
    mock_client = MagicMock()
    mock_client.invoke_model.return_value = {"body": mock_body}
    return mock_client


@pytest.fixture()
def aws_resources(s3_bucket, plans_table, schemas_table):
    return s3_bucket, plans_table, schemas_table


class TestPlanExcavatePipeline:
    def test_plan_id_flows_to_excavate(self, aws_resources):
        """plan_id returned by plan must be accepted by excavate with matching query."""
        # 1. Cache schema (simulates prior probe)
        cache_schema("athena:genomics.variants", SAMPLE_SCHEMA)

        # 2. Call plan with mocked Bedrock — capture plan_id
        with patch("tools.plan.handler.bedrock_runtime", return_value=_bedrock_mock(SAMPLE_QUERY)):
            plan_resp = plan_handler(
                {
                    "source_id": "athena:genomics.variants",
                    "objective": "List gene names",
                },
                None,
            )
        assert plan_resp["statusCode"] == 200
        plan_body = json.loads(plan_resp["body"])
        assert plan_body["status"] == "ready"
        plan_id = plan_body["plan_id"]

        # 3. Excavate with correct plan_id + matching query → 200
        mock_result = {
            "status": "complete",
            "rows": [{"gene": "BRCA1", "chromosome": "17"}],
            "bytes_scanned": 512,
            "cost": "$0.0000",
        }
        with patch.dict(
            "tools.excavate.handler.EXECUTORS",
            {"athena_sql": lambda **kw: mock_result},
        ):
            exc_resp = excavate_handler(
                {
                    "plan_id": plan_id,
                    "source_id": "athena:genomics.variants",
                    "query": SAMPLE_QUERY,
                    "query_type": "athena_sql",
                },
                None,
            )
        assert exc_resp["statusCode"] == 200

        # 4. Excavate with same plan_id but tampered query → 403
        with patch.dict(
            "tools.excavate.handler.EXECUTORS",
            {"athena_sql": lambda **kw: mock_result},
        ):
            tampered = excavate_handler(
                {
                    "plan_id": plan_id,
                    "source_id": "athena:genomics.variants",
                    "query": "SELECT * FROM genomics.variants",  # different
                    "query_type": "athena_sql",
                },
                None,
            )
        assert tampered["statusCode"] == 403

    def test_plan_without_prior_probe_fails(self, aws_resources):
        """plan handler returns 422 when schema not in DynamoDB."""
        resp = plan_handler(
            {
                "source_id": "athena:unknown.table",
                "objective": "Get all data",
            },
            None,
        )
        assert resp["statusCode"] == 422

    def test_result_stored_in_s3_after_excavate(self, aws_resources):
        """Successful excavate stores result JSON in S3 at {run_id}/result.json."""
        cache_schema("athena:genomics.variants", SAMPLE_SCHEMA)

        with patch("tools.plan.handler.bedrock_runtime", return_value=_bedrock_mock(SAMPLE_QUERY)):
            plan_resp = plan_handler(
                {"source_id": "athena:genomics.variants", "objective": "List genes"},
                None,
            )
        plan_id = json.loads(plan_resp["body"])["plan_id"]

        mock_rows = [{"gene": "BRCA1"}, {"gene": "TP53"}]
        with patch.dict(
            "tools.excavate.handler.EXECUTORS",
            {
                "athena_sql": lambda **kw: {
                    "status": "complete",
                    "rows": mock_rows,
                    "bytes_scanned": 256,
                    "cost": "$0.0000",
                }
            },
        ):
            exc_resp = excavate_handler(
                {
                    "plan_id": plan_id,
                    "source_id": "athena:genomics.variants",
                    "query": SAMPLE_QUERY,
                    "query_type": "athena_sql",
                },
                None,
            )
        assert exc_resp["statusCode"] == 200
        run_id = json.loads(exc_resp["body"])["run_id"]

        # Verify result stored in S3
        s3 = boto3.client("s3", region_name="us-east-1")
        obj = s3.get_object(Bucket="claws-runs", Key=f"{run_id}/result.json")
        stored = json.loads(obj["Body"].read())
        assert stored[0]["gene"] == "BRCA1"
