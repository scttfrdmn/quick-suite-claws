"""End-to-end pipeline tests: probe → plan → excavate → refine → export.

Validates the core plan_id bait-and-switch protection across handler boundaries
and the full 6-tool pipeline chain with real substrate AWS services.
"""

import contextlib
import json
from unittest.mock import MagicMock, patch

import boto3
import pytest

import tools.export.handler as _export_mod
import tools.mcp.registry as _reg_mod
import tools.probe.handler as _probe_mod
from tools.discover.handler import handler as discover_handler
from tools.excavate.handler import handler as excavate_handler
from tools.export.handler import handler as export_handler
from tools.plan.handler import handler as plan_handler
from tools.probe.handler import handler as probe_handler
from tools.refine.handler import handler as refine_handler
from tools.shared import cache_schema, new_run_id, store_result

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


# ---------------------------------------------------------------------------
# Shared helpers for TestFullPipeline
# ---------------------------------------------------------------------------

_GLUE_DB = "genomics_db"
_GLUE_TABLE = "variants_hg38"
_FULL_SOURCE_ID = f"athena:{_GLUE_DB}.{_GLUE_TABLE}"
_FULL_SCHEMA = {
    "database": _GLUE_DB,
    "table": _GLUE_TABLE,
    "columns": [
        {"name": "gene", "type": "string"},
        {"name": "chromosome", "type": "string"},
        {"name": "score", "type": "float"},
    ],
    "size_bytes_estimate": 500_000_000,
}
_FULL_QUERY = f"SELECT gene, chromosome FROM {_GLUE_DB}.{_GLUE_TABLE} LIMIT 10"
_MOCK_ROWS = [
    {"gene": "BRCA1", "chromosome": "17", "score": 0.9},
    {"gene": "TP53", "chromosome": "17", "score": 0.8},
]
_MOCK_EXEC = {
    "status": "complete",
    "rows": _MOCK_ROWS,
    "bytes_scanned": 1024,
    "cost": "$0.0050",
}

_MOCK_GLUE_RESPONSE = {
    "Table": {
        "Name": _GLUE_TABLE,
        "DatabaseName": _GLUE_DB,
        "StorageDescriptor": {
            "Columns": [
                {"Name": "gene", "Type": "string"},
                {"Name": "chromosome", "Type": "string"},
                {"Name": "score", "Type": "float"},
            ],
            "Location": f"s3://data/{_GLUE_DB}/{_GLUE_TABLE}/",
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": (
                "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
            ),
            "SerdeInfo": {
                "SerializationLibrary": (
                    "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
                )
            },
        },
        "PartitionKeys": [],
        "Parameters": {"recordCount": "500000"},
    }
}


def _mock_glue() -> MagicMock:
    """Glue client mock: get_databases + get_tables + get_table."""
    g = MagicMock()
    g.get_databases.return_value = {
        "DatabaseList": [{"Name": _GLUE_DB, "Parameters": {"claws:space": "research"}}]
    }
    g.get_tables.return_value = {"TableList": [{"Name": _GLUE_TABLE, "Description": "variants"}]}
    g.get_table.return_value = _MOCK_GLUE_RESPONSE
    return g


class TestFullPipeline:
    """Full 6-tool pipeline integration tests — issue #33."""

    @pytest.fixture(autouse=True)
    def reset_export_client(self):
        _export_mod.EVENTS_CLIENT = None
        _probe_mod.GLUE_CLIENT = None
        _probe_mod.ATHENA_CLIENT = None
        yield
        _export_mod.EVENTS_CLIENT = None

    def test_discover_to_probe_to_plan(self, plans_table, schemas_table):
        """discover → probe → plan: schema cached after probe; plan stored after plan."""
        mock_glue = _mock_glue()

        with patch("tools.discover.handler.glue_client", return_value=mock_glue):
            disc_resp = discover_handler(
                {
                    "query": "variants",
                    "scope": {"domains": ["athena"], "spaces": ["research"]},
                    "limit": 5,
                },
                None,
            )
        assert disc_resp["statusCode"] == 200
        sources = json.loads(disc_resp["body"])["sources"]
        assert any(_FULL_SOURCE_ID in s["id"] for s in sources)

        with patch("tools.probe.handler.glue_client", return_value=mock_glue):
            probe_resp = probe_handler(
                {"source_id": _FULL_SOURCE_ID, "mode": "schema_only"}, None
            )
        assert probe_resp["statusCode"] == 200
        assert "schema" in json.loads(probe_resp["body"])

        with patch("tools.plan.handler.bedrock_runtime", return_value=_bedrock_mock(_FULL_QUERY)):
            plan_resp = plan_handler(
                {"source_id": _FULL_SOURCE_ID, "objective": "List variant genes"},
                None,
            )
        assert plan_resp["statusCode"] == 200
        plan_body = json.loads(plan_resp["body"])
        assert plan_body["status"] == "ready"
        assert "plan_id" in plan_body

    def test_full_athena_pipeline(self, aws_resources):
        """Full chain: discover → probe → plan → excavate → refine → export."""
        cache_schema(_FULL_SOURCE_ID, _FULL_SCHEMA)

        with patch("tools.plan.handler.bedrock_runtime", return_value=_bedrock_mock(_FULL_QUERY)):
            plan_resp = plan_handler(
                {"source_id": _FULL_SOURCE_ID, "objective": "List variant genes"}, None
            )
        plan_body = json.loads(plan_resp["body"])
        plan_id = plan_body["plan_id"]

        with patch.dict(
            "tools.excavate.handler.EXECUTORS", {"athena_sql": lambda **kw: _MOCK_EXEC}
        ):
            exc_resp = excavate_handler(
                {
                    "plan_id": plan_id,
                    "source_id": _FULL_SOURCE_ID,
                    "query": _FULL_QUERY,
                    "query_type": "athena_sql",
                },
                None,
            )
        assert exc_resp["statusCode"] == 200
        run_id = json.loads(exc_resp["body"])["run_id"]

        ref_resp = refine_handler({"run_id": run_id, "operations": ["dedupe", "rank"]}, None)
        assert ref_resp["statusCode"] == 200
        refined_run_id = json.loads(ref_resp["body"])["run_id"]

        exp_resp = export_handler(
            {
                "run_id": refined_run_id,
                "destination": {"type": "s3", "uri": "s3://claws-runs/pipeline-final.json"},
            },
            None,
        )
        assert exp_resp["statusCode"] == 200
        exp_body = json.loads(exp_resp["body"])
        assert exp_body["status"] == "complete"
        assert exp_body["destination_uri"] == "s3://claws-runs/pipeline-final.json"

    def test_guardrail_block_stops_export(self, aws_resources):
        """Guardrail block during excavate propagates to export."""
        cache_schema(_FULL_SOURCE_ID, _FULL_SCHEMA)

        with patch("tools.plan.handler.bedrock_runtime", return_value=_bedrock_mock(_FULL_QUERY)):
            plan_resp = plan_handler(
                {"source_id": _FULL_SOURCE_ID, "objective": "List genes"}, None
            )
        plan_id = json.loads(plan_resp["body"])["plan_id"]

        # Excavate with guardrail blocking
        with (
            patch.dict("tools.excavate.handler.EXECUTORS", {"athena_sql": lambda **kw: _MOCK_EXEC}),
            patch("tools.excavate.handler.scan_payload", return_value={"status": "blocked"}),
        ):
            exc_resp = excavate_handler(
                {
                    "plan_id": plan_id,
                    "source_id": _FULL_SOURCE_ID,
                    "query": _FULL_QUERY,
                    "query_type": "athena_sql",
                },
                None,
            )
        assert exc_resp["statusCode"] == 200
        exc_body = json.loads(exc_resp["body"])
        assert exc_body["status"] == "blocked"
        assert exc_body["rows_returned"] == 0

        # Export of the blocked run also blocked (raw result stored for audit but scan blocks it)
        run_id = exc_body["run_id"]
        with patch("tools.export.handler.scan_payload", return_value={"status": "blocked"}):
            exp_resp = export_handler(
                {
                    "run_id": run_id,
                    "destination": {"type": "s3", "uri": "s3://claws-runs/blocked.json"},
                },
                None,
            )
        assert exp_resp["statusCode"] == 200
        assert json.loads(exp_resp["body"])["status"] == "blocked"

    def test_bait_and_switch_in_pipeline(self, aws_resources):
        """Probe → plan → excavate correct → 200; same plan + tampered query → 403."""
        cache_schema(_FULL_SOURCE_ID, _FULL_SCHEMA)

        with patch("tools.plan.handler.bedrock_runtime", return_value=_bedrock_mock(_FULL_QUERY)):
            plan_resp = plan_handler(
                {"source_id": _FULL_SOURCE_ID, "objective": "List genes"}, None
            )
        plan_id = json.loads(plan_resp["body"])["plan_id"]

        # Correct query accepted
        with patch.dict(
            "tools.excavate.handler.EXECUTORS", {"athena_sql": lambda **kw: _MOCK_EXEC}
        ):
            ok = excavate_handler(
                {
                    "plan_id": plan_id,
                    "source_id": _FULL_SOURCE_ID,
                    "query": _FULL_QUERY,
                    "query_type": "athena_sql",
                },
                None,
            )
        assert ok["statusCode"] == 200

        # Tampered query rejected
        with patch.dict(
            "tools.excavate.handler.EXECUTORS", {"athena_sql": lambda **kw: _MOCK_EXEC}
        ):
            tampered = excavate_handler(
                {
                    "plan_id": plan_id,
                    "source_id": _FULL_SOURCE_ID,
                    "query": "DROP TABLE genomics.variants",
                    "query_type": "athena_sql",
                },
                None,
            )
        assert tampered["statusCode"] == 403

    def test_refine_then_export(self, aws_resources):
        """Seed result → refine (dedupe + rank) → export to S3 with provenance."""
        run_id = new_run_id()
        rows = [
            {"gene": "BRCA1", "score": 0.9},
            {"gene": "BRCA1", "score": 0.9},  # duplicate
            {"gene": "TP53", "score": 0.8},
        ]
        store_result(run_id, rows)

        ref_resp = refine_handler({"run_id": run_id, "operations": ["dedupe", "rank"]}, None)
        assert ref_resp["statusCode"] == 200
        ref_body = json.loads(ref_resp["body"])
        assert ref_body["manifest"]["operations"][0]["rows_after"] == 2  # dedupe removed 1

        refined_id = ref_body["run_id"]
        exp_resp = export_handler(
            {
                "run_id": refined_id,
                "destination": {"type": "s3", "uri": "s3://claws-runs/refined-export.json"},
                "include_provenance": True,
            },
            None,
        )
        assert exp_resp["statusCode"] == 200
        exp_body = json.loads(exp_resp["body"])
        assert exp_body["status"] == "complete"
        assert "provenance_uri" in exp_body

        # Verify the exported file exists in S3
        s3 = boto3.client("s3", region_name="us-east-1")
        obj = s3.get_object(Bucket="claws-runs", Key="refined-export.json")
        exported = json.loads(obj["Body"].read())
        assert len(exported) == 2

    def test_mcp_pipeline(self, aws_resources, monkeypatch):
        """MCP discover → probe → plan → excavate: query_type=mcp_tool, zero cost."""
        server_config = {"transport": "stdio", "command": "npx @dbhub/mcp"}
        monkeypatch.setattr(_reg_mod, "_MODULE_REGISTRY", {"pg": server_config})

        # Probe: run_mcp_async called once for list_tools_and_resources
        tool_mock = MagicMock()
        tool_mock.name = "query"
        tool_mock.description = "Run SQL"
        tool_mock.inputSchema = {"type": "object", "properties": {"sql": {"type": "string"}}}
        resource_mock = MagicMock()
        resource_mock.name = "public.users"
        resource_mock.uri = "pg://public/users"
        resource_mock.description = "Users"
        mcp_source_id = "mcp://pg/public.users"

        with patch("tools.mcp.client.run_mcp_async", return_value=([tool_mock], [resource_mock])):
            probe_resp = probe_handler({"source_id": mcp_source_id, "mode": "schema_only"}, None)
        assert probe_resp["statusCode"] == 200
        assert "schema" in json.loads(probe_resp["body"])

        # Plan: Bedrock generates an MCP JSON query
        mcp_query = json.dumps({
            "server": "pg",
            "tool": "query",
            "arguments": {"sql": "SELECT * FROM public.users LIMIT 10"},
        })
        with patch("tools.plan.handler.bedrock_runtime", return_value=_bedrock_mock(mcp_query)):
            plan_resp = plan_handler(
                {"source_id": mcp_source_id, "objective": "List users"}, None
            )
        assert plan_resp["statusCode"] == 200
        plan_body = json.loads(plan_resp["body"])
        assert plan_body["status"] == "ready"
        assert plan_body["steps"][0]["input"]["query_type"] == "mcp_tool"
        assert plan_body["estimated_cost"] == "$0.00"
        plan_id = plan_body["plan_id"]

        # Excavate: run_mcp_async returns rows
        with patch("tools.mcp.client.run_mcp_async", return_value=[{"id": 1, "name": "Alice"}]):
            exc_resp = excavate_handler(
                {
                    "plan_id": plan_id,
                    "source_id": mcp_source_id,
                    "query": mcp_query,
                    "query_type": "mcp_tool",
                },
                None,
            )
        assert exc_resp["statusCode"] == 200
        exc_body = json.loads(exc_resp["body"])
        assert exc_body["rows_returned"] == 1
        assert exc_body["cost"] == "$0.0000"

    def test_export_eventbridge(self, s3_bucket):
        """Seed result and export to EventBridge; assert put_events called."""
        run_id = new_run_id()
        store_result(run_id, _MOCK_ROWS)

        mock_eb = MagicMock()
        mock_eb.put_events.return_value = {"FailedEntryCount": 0, "Entries": [{"EventId": "e1"}]}

        with patch("tools.export.handler._events_client", return_value=mock_eb):
            exp_resp = export_handler(
                {
                    "run_id": run_id,
                    "destination": {
                        "type": "eventbridge",
                        "uri": "events://claws-bus/ClawsExportReady",
                    },
                },
                None,
            )
        assert exp_resp["statusCode"] == 200
        assert json.loads(exp_resp["body"])["status"] == "complete"
        mock_eb.put_events.assert_called_once()
        entry = mock_eb.put_events.call_args[1]["Entries"][0]
        assert entry["EventBusName"] == "claws-bus"
        assert entry["DetailType"] == "ClawsExportReady"

    def test_schema_cache_required_for_plan(self, plans_table, schemas_table):
        """Plan without prior probe → 422; probe then plan → 200."""
        resp = plan_handler(
            {"source_id": "athena:fresh_db.fresh_table", "objective": "Get data"},
            None,
        )
        assert resp["statusCode"] == 422

        cache_schema("athena:fresh_db.fresh_table", SAMPLE_SCHEMA)
        with patch("tools.plan.handler.bedrock_runtime", return_value=_bedrock_mock(SAMPLE_QUERY)):
            resp2 = plan_handler(
                {"source_id": "athena:fresh_db.fresh_table", "objective": "Get data"},
                None,
            )
        assert resp2["statusCode"] == 200
        assert json.loads(resp2["body"])["status"] == "ready"

    def test_refine_with_all_operations(self, s3_bucket):
        """dedupe + rank_by_score + filter + normalize all applied; manifest tracks row counts."""
        run_id = new_run_id()
        rows = [
            {"Gene": "BRCA1", "score": "0.9", "active": True},
            {"Gene": "BRCA1", "score": "0.9", "active": True},  # duplicate
            {"Gene": "TP53", "score": "0.8", "active": False},
            {"Gene": "KRAS", "score": "0.7", "active": True},
        ]
        store_result(run_id, rows)

        ref_resp = refine_handler(
            {
                "run_id": run_id,
                "operations": [
                    "dedupe",
                    "rank_by_score",
                    {"op": "filter", "field": "active", "operator": "eq", "value": True},
                    "normalize",
                ],
            },
            None,
        )
        assert ref_resp["statusCode"] == 200
        body = json.loads(ref_resp["body"])
        ops = body["manifest"]["operations"]
        assert len(ops) == 4
        # Each op's rows_after <= rows_before
        for op in ops:
            assert op["rows_after"] <= op["rows_before"]
        # dedupe: 4 → 3
        assert ops[0]["rows_after"] == 3

    def test_pipeline_audit_trail(self, aws_resources, capsys):
        """plan + excavate both emit structured JSON audit records to stdout."""
        cache_schema(_FULL_SOURCE_ID, _FULL_SCHEMA)

        with patch("tools.plan.handler.bedrock_runtime", return_value=_bedrock_mock(_FULL_QUERY)):
            plan_handler(
                {"source_id": _FULL_SOURCE_ID, "objective": "Audit test"},
                None,
            )

        with patch.dict(
            "tools.excavate.handler.EXECUTORS", {"athena_sql": lambda **kw: _MOCK_EXEC}
        ):
            excavate_handler(
                {
                    "source_id": _FULL_SOURCE_ID,
                    "query": _FULL_QUERY,
                    "query_type": "athena_sql",
                },
                None,
            )

        captured = capsys.readouterr()
        records = []
        for line in captured.out.splitlines():
            with contextlib.suppress(json.JSONDecodeError):
                records.append(json.loads(line))

        tools_seen = {r.get("tool") for r in records}
        assert "plan" in tools_seen
        assert "excavate" in tools_seen
        for r in records:
            if r.get("tool") in ("plan", "excavate"):
                assert "principal" in r
                assert "timestamp" in r
