"""Direct unit tests for execute_athena() and execute_s3_select() executors."""

from unittest.mock import MagicMock

import pytest

import tools.excavate.executors.athena as _athena_mod
from tools.excavate.executors.athena import execute_athena
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
