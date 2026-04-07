"""Handler-level tests for claws.excavate using substrate S3 + DynamoDB."""

import json
from unittest.mock import MagicMock, patch

import boto3
import pytest

import tools.excavate.executors.opensearch as _os_mod
from tools.excavate.handler import handler
from tools.shared import store_plan


@pytest.fixture(autouse=True)
def reset_os_client():
    """Reset per-endpoint OpenSearch client cache between tests."""
    _os_mod.OS_CLIENT.clear()
    yield
    _os_mod.OS_CLIENT.clear()


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


class TestResultMetadata:
    def test_metadata_written_alongside_result(self, aws_resources):
        """After successful excavate, result_metadata.json is written to S3."""
        mock_result = {
            "status": "complete",
            "rows": [{"gene": "BRCA1", "cohort": "2024", "n": 42}],
            "bytes_scanned": 237123584,
            "cost": "$0.22",
        }

        with patch.dict(
            "tools.excavate.handler.EXECUTORS",
            {"athena_sql": lambda **kw: mock_result},
        ):
            resp = handler(
                {
                    "source_id": "athena:oncology.variant_index",
                    "query": "SELECT gene, cohort, n FROM oncology.variant_index",
                    "query_type": "athena_sql",
                },
                None,
            )

        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert "metadata_uri" in body

        s3 = boto3.client("s3", region_name="us-east-1")
        obj = s3.get_object(Bucket="claws-runs", Key=f"{body['run_id']}/result_metadata.json")
        meta = json.loads(obj["Body"].read())

        assert meta["run_id"] == body["run_id"]
        assert meta["row_count"] == 1
        assert meta["bytes_scanned"] == 237123584
        assert meta["cost"] == "$0.22"
        assert meta["source_id"] == "athena:oncology.variant_index"
        assert "created_at" in meta

    def test_schema_inferred_from_rows(self, aws_resources):
        """Schema in metadata reflects actual column names and types from rows."""
        mock_result = {
            "status": "complete",
            "rows": [{"gene": "BRCA1", "n": 42, "score": 0.95, "active": True}],
            "bytes_scanned": 1024,
            "cost": "$0.00",
        }

        with patch.dict(
            "tools.excavate.handler.EXECUTORS",
            {"athena_sql": lambda **kw: mock_result},
        ):
            resp = handler(
                {
                    "source_id": "athena:test.table",
                    "query": "SELECT gene, n, score, active FROM test.table",
                    "query_type": "athena_sql",
                },
                None,
            )

        body = json.loads(resp["body"])
        s3 = boto3.client("s3", region_name="us-east-1")
        obj = s3.get_object(Bucket="claws-runs", Key=f"{body['run_id']}/result_metadata.json")
        meta = json.loads(obj["Body"].read())

        schema = {col["name"]: col["type"] for col in meta["schema"]}
        assert schema["gene"] == "string"
        assert schema["n"] == "bigint"
        assert schema["score"] == "double"
        assert schema["active"] == "boolean"

    def test_metadata_readable_when_no_rows(self, aws_resources):
        """Empty result set → schema is [] and metadata is still written."""
        mock_result = {
            "status": "complete",
            "rows": [],
            "bytes_scanned": 0,
            "cost": "$0.00",
        }

        with patch.dict(
            "tools.excavate.handler.EXECUTORS",
            {"athena_sql": lambda **kw: mock_result},
        ):
            resp = handler(
                {
                    "source_id": "athena:empty.table",
                    "query": "SELECT * FROM empty.table WHERE 1=0",
                    "query_type": "athena_sql",
                },
                None,
            )

        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        s3 = boto3.client("s3", region_name="us-east-1")
        obj = s3.get_object(Bucket="claws-runs", Key=f"{body['run_id']}/result_metadata.json")
        meta = json.loads(obj["Body"].read())

        assert meta["schema"] == []
        assert meta["row_count"] == 0


class TestOpenSearchExecutor:
    """Tests for execute_opensearch — all mock _os_client since substrate
    has no OpenSearch plugin."""

    def test_parse_source_id_valid(self):
        from tools.excavate.executors.opensearch import _parse_source_id
        endpoint, index = _parse_source_id(
            "opensearch:search-prod.us-east-1.es.amazonaws.com/genes"
        )
        assert endpoint == "search-prod.us-east-1.es.amazonaws.com"
        assert index == "genes"

    def test_parse_source_id_missing_index(self):
        from tools.excavate.executors.opensearch import _parse_source_id
        with pytest.raises(ValueError, match="Invalid opensearch source_id"):
            _parse_source_id("opensearch:search-prod.us-east-1.es.amazonaws.com")

    def test_parse_source_id_wrong_prefix(self):
        from tools.excavate.executors.opensearch import _parse_source_id
        with pytest.raises(ValueError):
            _parse_source_id("athena:db.table")

    def test_execute_complete(self, monkeypatch):
        mock_response = {
            "hits": {
                "total": {"value": 2},
                "hits": [
                    {"_source": {"gene": "BRCA1", "score": 0.9}},
                    {"_source": {"gene": "TP53", "score": 0.7}},
                ],
            }
        }
        mock_client = MagicMock()
        mock_client.search.return_value = mock_response
        monkeypatch.setattr(_os_mod, "_os_client", lambda endpoint: mock_client)

        from tools.excavate.executors.opensearch import execute_opensearch
        result = execute_opensearch(
            source_id="opensearch:search-prod.us-east-1.es.amazonaws.com/genes",
            query='{"query": {"match_all": {}}}',
            constraints={"max_rows": 50},
            run_id="run-test0001",
        )

        assert result["status"] == "complete"
        assert len(result["rows"]) == 2
        assert result["rows"][0]["gene"] == "BRCA1"
        assert result["cost"] == "$0.0000"
        assert result["bytes_scanned"] == 0
        call_body = mock_client.search.call_args[1]["body"]
        assert call_body["size"] == 50

    def test_execute_query_as_dict(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.search.return_value = {"hits": {"hits": []}}
        monkeypatch.setattr(_os_mod, "_os_client", lambda endpoint: mock_client)

        from tools.excavate.executors.opensearch import execute_opensearch
        result = execute_opensearch(
            source_id="opensearch:host.es.amazonaws.com/idx",
            query={"query": {"match": {"field": "value"}}},
            constraints={},
            run_id="run-00000001",
        )
        assert result["status"] == "complete"

    def test_execute_size_capped_at_1000(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.search.return_value = {"hits": {"hits": []}}
        monkeypatch.setattr(_os_mod, "_os_client", lambda endpoint: mock_client)

        from tools.excavate.executors.opensearch import execute_opensearch
        execute_opensearch(
            source_id="opensearch:host.es.amazonaws.com/idx",
            query="{}",
            constraints={"max_rows": 5000},
            run_id="run-00000002",
        )
        call_body = mock_client.search.call_args[1]["body"]
        assert call_body["size"] == 1000

    def test_execute_timeout(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.search.side_effect = Exception("Connection timed out")
        monkeypatch.setattr(_os_mod, "_os_client", lambda endpoint: mock_client)

        from tools.excavate.executors.opensearch import execute_opensearch
        result = execute_opensearch(
            source_id="opensearch:host.es.amazonaws.com/idx",
            query="{}",
            constraints={"timeout_seconds": 5},
            run_id="run-timeout01",
        )
        assert result["status"] == "timeout"
        assert "timed out" in result["error"]

    def test_execute_invalid_json_query(self, monkeypatch):
        from tools.excavate.executors.opensearch import execute_opensearch
        result = execute_opensearch(
            source_id="opensearch:host.es.amazonaws.com/idx",
            query="not json {{{",
            constraints={},
            run_id="run-00000003",
        )
        assert result["status"] == "error"
        assert "not valid JSON" in result["error"]

    def test_execute_bad_source_id(self):
        from tools.excavate.executors.opensearch import execute_opensearch
        result = execute_opensearch(
            source_id="opensearch:no-slash-here",
            query="{}",
            constraints={},
            run_id="run-00000004",
        )
        assert result["status"] == "error"
        assert "Invalid opensearch source_id" in result["error"]

    def test_handler_routes_opensearch_dsl(self, aws_resources, monkeypatch):
        """Excavate handler routes opensearch_dsl query_type to execute_opensearch."""
        mock_result = {
            "status": "complete",
            "rows": [{"doc_id": "abc", "score": 0.95}],
            "bytes_scanned": 0,
            "cost": "$0.0000",
        }
        with patch.dict(
            "tools.excavate.handler.EXECUTORS",
            {"opensearch_dsl": lambda **kw: mock_result},
        ):
            resp = handler(
                {
                    "source_id": "opensearch:search-prod.us-east-1.es.amazonaws.com/genes",
                    "query": '{"query": {"match_all": {}}}',
                    "query_type": "opensearch_dsl",
                },
                None,
            )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["status"] == "complete"
        assert body["rows_returned"] == 1
