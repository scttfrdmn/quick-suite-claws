"""Direct unit tests for execute_athena(), execute_s3_select(), and execute_opensearch()."""

from unittest.mock import MagicMock

import pytest

import tools.excavate.executors.athena as _athena_mod
import tools.excavate.executors.opensearch as _os_mod
from tools.excavate.executors.athena import execute_athena
from tools.excavate.executors.opensearch import _flatten_aggregations, execute_opensearch
from tools.excavate.executors.s3_select import (
    _input_serialization,
    _parse_source_id,
    execute_s3_select,
)


@pytest.fixture(autouse=True)
def reset_athena_clients():
    """Reset module-level Athena/S3 singletons so substrate intercepts them."""
    _athena_mod.ATHENA_CLIENT = None
    _athena_mod.S3_CLIENT = None
    yield
    _athena_mod.ATHENA_CLIENT = None
    _athena_mod.S3_CLIENT = None


class TestAthenaExecutor:
    def test_execute_athena_complete(self, substrate):
        """Real Athena query execution against substrate."""
        result = execute_athena(
            source_id="athena:testdb.testtable",
            query="SELECT 1 AS n",
            constraints={},
            run_id="run-exec0001",
        )
        # substrate Athena may return empty rows for SELECT 1, but status must be complete
        assert result["status"] == "complete"
        assert isinstance(result["bytes_scanned"], int)
        assert result["cost"].startswith("$")

    def test_execute_athena_query_failed(self, monkeypatch):
        """Executor returns status=error when query state is FAILED."""
        mock_client = MagicMock()
        mock_client.start_query_execution.return_value = {"QueryExecutionId": "qe-failed"}
        mock_client.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {"State": "FAILED", "StateChangeReason": "Syntax error near SELECT"},
                "Statistics": {},
            }
        }
        monkeypatch.setattr(_athena_mod, "ATHENA_CLIENT", mock_client)

        result = execute_athena(
            source_id="athena:db.t",
            query="BAD QUERY",
            constraints={},
            run_id="run-exec0002",
        )
        assert result["status"] == "error"
        assert "Syntax error" in result["error"]

    def test_execute_athena_timeout(self, monkeypatch):
        """Executor returns status=timeout when timeout_seconds is exceeded."""
        mock_client = MagicMock()
        mock_client.start_query_execution.return_value = {"QueryExecutionId": "qe-slow"}
        mock_client.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "RUNNING"}, "Statistics": {}}
        }
        monkeypatch.setattr(_athena_mod, "ATHENA_CLIENT", mock_client)

        result = execute_athena(
            source_id="athena:db.t",
            query="SELECT SLEEP(999)",
            constraints={"timeout_seconds": 0},
            run_id="run-exec0003",
        )
        assert result["status"] == "timeout"
        assert "timed out" in result["error"]

    def test_execute_athena_start_failure(self, monkeypatch):
        """Executor returns status=error when start_query_execution raises."""
        mock_client = MagicMock()
        mock_client.start_query_execution.side_effect = Exception("Access denied")
        monkeypatch.setattr(_athena_mod, "ATHENA_CLIENT", mock_client)

        result = execute_athena(
            source_id="athena:db.t",
            query="SELECT 1",
            constraints={},
            run_id="run-exec0004",
        )
        assert result["status"] == "error"
        assert "Access denied" in result["error"]

    def test_execute_athena_cost_calculation(self, monkeypatch):
        """1 TB scanned → cost == $5.0000."""
        one_tb = 1024 ** 4  # bytes
        mock_client = MagicMock()
        mock_client.start_query_execution.return_value = {"QueryExecutionId": "qe-cost"}
        mock_client.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {"State": "SUCCEEDED"},
                "Statistics": {"DataScannedInBytes": one_tb},
            }
        }

        # Mock paginator to return one page with one row
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {
                "ResultSet": {
                    "ResultSetMetadata": {"ColumnInfo": [{"Name": "n"}]},
                    "Rows": [
                        {"Data": [{"VarCharValue": "n"}]},   # header row
                        {"Data": [{"VarCharValue": "1"}]},   # data row
                    ],
                }
            }
        ]
        mock_client.get_paginator.return_value = mock_paginator
        monkeypatch.setattr(_athena_mod, "ATHENA_CLIENT", mock_client)

        result = execute_athena(
            source_id="athena:db.t",
            query="SELECT * FROM big_table",
            constraints={},
            run_id="run-exec0005",
        )
        assert result["status"] == "complete"
        assert result["cost"] == "$5.0000"
        assert result["bytes_scanned"] == one_tb


class TestS3SelectExecutor:
    def test_parse_source_id_s3_uri(self):
        bucket, key = _parse_source_id("s3://my-bucket/data/file.csv")
        assert bucket == "my-bucket"
        assert key == "data/file.csv"

    def test_parse_source_id_s3_colon(self):
        bucket, key = _parse_source_id("s3:my-bucket/data/file.csv")
        assert bucket == "my-bucket"
        assert key == "data/file.csv"

    def test_parse_source_id_missing_key(self):
        """Missing key → execute_s3_select returns status=error (no ValueError raised)."""
        result = execute_s3_select(
            source_id="s3://bucket-only",
            query="SELECT * FROM S3Object",
            constraints={},
            run_id="run-s3s0001",
        )
        assert result["status"] == "error"
        assert "missing" in result["error"].lower() or "invalid" in result["error"].lower()

    def test_input_serialization_csv(self):
        ser = _input_serialization("data.csv", {})
        assert "CSV" in ser

    def test_input_serialization_json(self):
        ser = _input_serialization("records.json", {})
        assert "JSON" in ser

    def test_input_serialization_parquet(self):
        ser = _input_serialization("table.parquet", {})
        assert "Parquet" in ser

    def test_execute_s3_select_csv(self, s3_bucket):
        """Real S3 Select against substrate — CSV input."""
        csv = "gene,score\nBRCA1,0.9\nTP53,0.7\n"
        s3_bucket.put_object(Bucket="claws-runs", Key="test/genes.csv", Body=csv.encode())

        # Reset s3_select's shared s3_client so substrate intercepts it
        import tools.shared as _shared
        _shared._s3 = None

        result = execute_s3_select(
            source_id="s3://claws-runs/test/genes.csv",
            query="SELECT * FROM S3Object",
            constraints={},
            run_id="run-s3s0002",
        )
        assert result["status"] == "complete"
        assert len(result["rows"]) == 2
        genes = [r["gene"] for r in result["rows"]]
        assert "BRCA1" in genes
        assert result["cost"].startswith("$")

    def test_execute_s3_select_no_such_key(self, s3_bucket):
        """Missing S3 key returns status=error."""
        import tools.shared as _shared
        _shared._s3 = None

        result = execute_s3_select(
            source_id="s3://claws-runs/nonexistent/file.csv",
            query="SELECT * FROM S3Object",
            constraints={},
            run_id="run-s3s0003",
        )
        assert result["status"] == "error"
        assert "not found" in result["error"].lower() or "NoSuchKey" in result["error"]


class TestOpenSearchExecutor:
    @pytest.fixture(autouse=True)
    def reset_os_clients(self):
        _os_mod.OS_CLIENT.clear()
        yield
        _os_mod.OS_CLIENT.clear()

    def _mock_client(self, response: dict) -> MagicMock:
        mock = MagicMock()
        mock.search.return_value = response
        return mock

    # --- _flatten_aggregations unit tests ---

    def test_flatten_single_level_terms_agg(self):
        aggs = {
            "by_service": {
                "buckets": [
                    {"key": "payment-svc", "doc_count": 847},
                    {"key": "auth-svc", "doc_count": 501},
                ]
            }
        }
        rows = _flatten_aggregations(aggs)
        assert len(rows) == 2
        assert rows[0] == {"by_service": "payment-svc", "count": 847}
        assert rows[1] == {"by_service": "auth-svc", "count": 501}

    def test_flatten_nested_terms_agg(self):
        """Two-level agg: matches the log-analysis example."""
        aggs = {
            "by_service": {
                "buckets": [
                    {
                        "key": "payment-svc",
                        "doc_count": 1159,
                        "top_messages": {
                            "buckets": [
                                {"key": "Upstream timeout after 5000ms", "doc_count": 847},
                                {"key": "Upstream timeout after 3000ms", "doc_count": 312},
                            ]
                        },
                    },
                    {
                        "key": "auth-svc",
                        "doc_count": 501,
                        "top_messages": {
                            "buckets": [
                                {"key": "Token validation failed", "doc_count": 501},
                            ]
                        },
                    },
                ]
            }
        }
        rows = _flatten_aggregations(aggs)
        assert len(rows) == 3
        assert rows[0] == {
            "by_service": "payment-svc",
            "top_messages": "Upstream timeout after 5000ms",
            "count": 847,
        }
        assert rows[2] == {
            "by_service": "auth-svc",
            "top_messages": "Token validation failed",
            "count": 501,
        }

    def test_flatten_empty_aggs(self):
        assert _flatten_aggregations({}) == []

    def test_flatten_no_bucket_aggs(self):
        # value_count or avg aggs (no 'buckets' key) → empty
        assert _flatten_aggregations({"total": {"value": 42}}) == []

    # --- execute_opensearch integration tests ---

    def test_execute_opensearch_hits(self, monkeypatch):
        """Non-aggregation query: rows come from hits.hits._source."""
        mock = self._mock_client({
            "hits": {
                "hits": [
                    {"_source": {"gene": "BRCA1", "score": 0.9}},
                    {"_source": {"gene": "TP53", "score": 0.8}},
                ]
            }
        })
        monkeypatch.setattr(_os_mod, "_os_client", lambda endpoint: mock)

        result = execute_opensearch(
            source_id="opensearch:search-prod.us-east-1.es.amazonaws.com/genes",
            query={"query": {"match_all": {}}},
            constraints={},
            run_id="run-os0001",
        )
        assert result["status"] == "complete"
        assert len(result["rows"]) == 2
        assert result["rows"][0]["gene"] == "BRCA1"
        assert result["cost"] == "$0.0000"
        assert result["bytes_scanned"] == 0

    def test_execute_opensearch_aggregation(self, monkeypatch):
        """Aggregation query: rows come from flattened buckets."""
        mock = self._mock_client({
            "hits": {"hits": []},
            "aggregations": {
                "by_service": {
                    "buckets": [
                        {"key": "payment-svc", "doc_count": 847},
                        {"key": "auth-svc", "doc_count": 501},
                    ]
                }
            },
        })
        monkeypatch.setattr(_os_mod, "_os_client", lambda endpoint: mock)

        result = execute_opensearch(
            source_id="opensearch:search-prod.us-east-1.es.amazonaws.com/logs",
            query={"size": 0, "aggs": {"by_service": {"terms": {"field": "service"}}}},
            constraints={},
            run_id="run-os0002",
        )
        assert result["status"] == "complete"
        assert len(result["rows"]) == 2
        assert result["rows"][0] == {"by_service": "payment-svc", "count": 847}

    def test_execute_opensearch_read_only_blocks_delete(self, monkeypatch):
        """read_only constraint blocks _delete_by_query in DSL."""
        monkeypatch.setattr(_os_mod, "_os_client", lambda endpoint: MagicMock())

        result = execute_opensearch(
            source_id="opensearch:search-prod.us-east-1.es.amazonaws.com/logs",
            query={"_delete_by_query": {"query": {"match_all": {}}}},
            constraints={"read_only": True},
            run_id="run-os0003",
        )
        assert result["status"] == "error"
        assert "_delete_by_query" in result["error"]

    def test_execute_opensearch_invalid_json_string(self, monkeypatch):
        """Malformed JSON query string returns status=error."""
        monkeypatch.setattr(_os_mod, "_os_client", lambda endpoint: MagicMock())

        result = execute_opensearch(
            source_id="opensearch:search-prod.us-east-1.es.amazonaws.com/logs",
            query="not valid json {{{",
            constraints={},
            run_id="run-os0004",
        )
        assert result["status"] == "error"
        assert "not valid JSON" in result["error"]

    def test_execute_opensearch_connection_error(self, monkeypatch):
        """Client exception returns status=error."""
        mock = MagicMock()
        mock.search.side_effect = Exception("connection refused")
        monkeypatch.setattr(_os_mod, "_os_client", lambda endpoint: mock)

        result = execute_opensearch(
            source_id="opensearch:search-prod.us-east-1.es.amazonaws.com/logs",
            query={"query": {"match_all": {}}},
            constraints={},
            run_id="run-os0005",
        )
        assert result["status"] == "error"
        assert "connection refused" in result["error"]

    def test_execute_opensearch_invalid_source_id(self):
        result = execute_opensearch(
            source_id="opensearch:no-slash-here",
            query={"query": {"match_all": {}}},
            constraints={},
            run_id="run-os0006",
        )
        assert result["status"] == "error"
