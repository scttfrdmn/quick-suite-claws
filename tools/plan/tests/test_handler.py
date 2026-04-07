"""Handler-level tests for claws.plan using substrate DynamoDB + mocked Bedrock."""

import json
from unittest.mock import MagicMock, patch

import pytest

from tools.plan.handler import handler
from tools.shared import load_plan

SAMPLE_SCHEMA = {
    "database": "genomics",
    "table": "variants",
    "columns": [{"name": "gene", "type": "string"}, {"name": "chromosome", "type": "string"}],
    "size_bytes_estimate": 1_000_000_000,  # 1 GB
}


def _seed_schema(schemas_table, source_id: str, schema: dict):
    schemas_table.put_item(Item={"source_id": source_id, "schema": schema})


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

    def test_requires_cached_schema(self, plans_table, schemas_table):
        """Without a prior probe, plan should fail with 422."""
        resp = handler({"objective": "find genes", "source_id": "athena:genomics.variants"}, None)
        assert resp["statusCode"] == 422

    def test_stores_plan_when_valid(self, plans_table, schemas_table):
        _seed_schema(schemas_table, "athena:genomics.variants", SAMPLE_SCHEMA)
        bedrock_client = _bedrock_mock("SELECT gene, chromosome FROM genomics.variants LIMIT 100")

        with patch("tools.plan.handler.bedrock_runtime", return_value=bedrock_client):
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

    def test_rejects_mutation_query(self, plans_table, schemas_table):
        _seed_schema(schemas_table, "athena:genomics.variants", SAMPLE_SCHEMA)
        bedrock_client = _bedrock_mock(
            "INSERT INTO genomics.variants SELECT * FROM genomics.variants"
        )

        with patch("tools.plan.handler.bedrock_runtime", return_value=bedrock_client):
            resp = handler(
                {"objective": "copy data", "source_id": "athena:genomics.variants"},
                None,
            )

        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["status"] == "rejected"
        assert any(w in body["reason"].lower() for w in ("mutation", "write", "insert", "select"))

    def test_rejects_excess_cost(self, plans_table, schemas_table):
        huge_schema = {**SAMPLE_SCHEMA, "size_bytes_estimate": 100_000_000_000_000}  # 100 TB
        _seed_schema(schemas_table, "athena:genomics.variants", huge_schema)
        bedrock_client = _bedrock_mock("SELECT * FROM genomics.variants")

        with patch("tools.plan.handler.bedrock_runtime", return_value=bedrock_client):
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


MCP_SCHEMA = {
    "server": "postgres-prod",
    "resource": "public.users",
    "description": "Users table",
    "available_tools": [
        {"name": "query", "description": "Run SQL", "input_schema": {"sql": "string"}},
    ],
}

_MCP_QUERY = json.dumps({
    "server": "postgres-prod",
    "tool": "query",
    "arguments": {"sql": "SELECT * FROM users LIMIT 10"},
})


class TestPlanMcp:
    @pytest.fixture(autouse=True)
    def _patch_mcp_registry(self):
        with patch("tools.mcp.registry.known_servers", return_value={"postgres-prod"}):
            yield

    def test_mcp_skips_sql_validator(self, plans_table, schemas_table):
        """MCP plans bypass SQL validation — query_type is mcp_tool."""
        _seed_schema(schemas_table, "mcp://postgres-prod/public.users", MCP_SCHEMA)
        bedrock_client = _bedrock_mock(_MCP_QUERY)

        with patch("tools.plan.handler.bedrock_runtime", return_value=bedrock_client):
            resp = handler(
                {
                    "objective": "get the first 10 users",
                    "source_id": "mcp://postgres-prod/public.users",
                },
                None,
            )

        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["status"] == "ready"
        assert body["steps"][0]["input"]["query_type"] == "mcp_tool"

    def test_mcp_plan_zero_cost(self, plans_table, schemas_table):
        """MCP plans always have zero estimated cost and bytes."""
        _seed_schema(schemas_table, "mcp://postgres-prod/public.users", MCP_SCHEMA)
        bedrock_client = _bedrock_mock(_MCP_QUERY)

        with patch("tools.plan.handler.bedrock_runtime", return_value=bedrock_client):
            resp = handler(
                {
                    "objective": "list users",
                    "source_id": "mcp://postgres-prod/public.users",
                },
                None,
            )

        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["estimated_cost"] == "$0.00"
        assert body["estimated_bytes_scanned"] == 0
