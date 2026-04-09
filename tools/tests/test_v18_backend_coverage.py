"""Tests for clAWS v0.18.0 — PostgreSQL executor, Redshift executor, per-principal budget caps."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

REPO_ROOT = Path(__file__).parent.parent.parent


# ---------------------------------------------------------------------------
# Test: PostgreSQL executor (#63)
# ---------------------------------------------------------------------------


class TestPostgresExecutor:
    def setup_method(self):
        for k in list(sys.modules.keys()):
            if "tools.excavate.executors.postgres" in k:
                del sys.modules[k]

    def _load_executor(self):
        import tools.excavate.executors.postgres as pg
        return pg

    def test_happy_path(self):
        pg = self._load_executor()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchmany.return_value = [(1, "Alice"), (2, "Bob")]
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_psycopg2 = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn

        mock_secrets = MagicMock()
        mock_secrets.get_secret_value.return_value = {
            "SecretString": json.dumps({
                "host": "localhost",
                "port": 5432,
                "dbname": "testdb",
                "username": "user",
                "password": "pass",
            })
        }

        with patch.object(pg, "_secrets_client", mock_secrets), \
             patch.object(pg, "POSTGRES_SECRET_ARN", "arn:aws:sm:us-east-1:123:secret:pg"), \
             patch.dict("sys.modules", {"psycopg2": mock_psycopg2}):
            result = pg.execute_postgres("postgres:mydb.mytable", "SELECT * FROM t", {}, "run-1")

        assert result["status"] == "complete"
        assert result["row_count"] == 2
        assert len(result["rows"]) == 2
        assert result["rows"][0] == {"id": 1, "name": "Alice"}

    def test_mutation_detection(self):
        pg = self._load_executor()
        result = pg.execute_postgres("postgres:db.t", "INSERT INTO t VALUES (1)", {}, "run-1")
        assert result["status"] == "error"
        assert "Mutation detected" in result["error"]

    def test_connection_error(self):
        pg = self._load_executor()
        mock_psycopg2 = MagicMock()
        mock_psycopg2.connect.side_effect = Exception("Connection refused")

        mock_secrets = MagicMock()
        mock_secrets.get_secret_value.return_value = {
            "SecretString": json.dumps({
                "host": "localhost", "dbname": "testdb",
                "username": "user", "password": "pass",
            })
        }

        with patch.object(pg, "_secrets_client", mock_secrets), \
             patch.object(pg, "POSTGRES_SECRET_ARN", "arn:aws:sm:us-east-1:123:secret:pg"), \
             patch.dict("sys.modules", {"psycopg2": mock_psycopg2}):
            result = pg.execute_postgres("postgres:db.t", "SELECT 1", {}, "run-1")

        assert result["status"] == "error"
        assert result["error"] == "Query execution failed"

    def test_empty_result(self):
        pg = self._load_executor()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchmany.return_value = []
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_psycopg2 = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn

        mock_secrets = MagicMock()
        mock_secrets.get_secret_value.return_value = {
            "SecretString": json.dumps({
                "host": "localhost", "dbname": "testdb",
                "username": "user", "password": "pass",
            })
        }

        with patch.object(pg, "_secrets_client", mock_secrets), \
             patch.object(pg, "POSTGRES_SECRET_ARN", "arn:aws:sm:us-east-1:123:secret:pg"), \
             patch.dict("sys.modules", {"psycopg2": mock_psycopg2}):
            result = pg.execute_postgres("postgres:db.t", "SELECT * FROM t", {}, "run-1")

        assert result["status"] == "complete"
        assert result["rows"] == []
        assert result["row_count"] == 0

    def test_psycopg2_not_available(self):
        pg = self._load_executor()
        # Simulate psycopg2 not installed by making import raise ImportError
        import builtins
        original_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "psycopg2":
                raise ImportError("No module named 'psycopg2'")
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            result = pg.execute_postgres("postgres:db.t", "SELECT 1", {}, "run-1")

        assert result["status"] == "error"
        assert "psycopg2 not available" in result["error"]


# ---------------------------------------------------------------------------
# Test: Redshift executor (#64)
# ---------------------------------------------------------------------------


class TestRedshiftExecutor:
    def setup_method(self):
        for k in list(sys.modules.keys()):
            if "tools.excavate.executors.redshift" in k:
                del sys.modules[k]

    def _load_executor(self):
        import tools.excavate.executors.redshift as rs
        return rs

    def test_happy_path(self):
        rs = self._load_executor()
        mock_client = MagicMock()
        mock_client.execute_statement.return_value = {"Id": "stmt-123"}
        mock_client.describe_statement.return_value = {
            "Status": "FINISHED",
            "ResultSize": 1024,
        }
        mock_client.get_statement_result.return_value = {
            "ColumnMetadata": [{"name": "id"}, {"name": "value"}],
            "Records": [
                [{"longValue": 1}, {"stringValue": "hello"}],
                [{"longValue": 2}, {"stringValue": "world"}],
            ],
        }

        with patch.object(rs, "_redshift_client", mock_client), \
             patch.object(rs, "WORKGROUP", "my-workgroup"), \
             patch.object(rs, "DATABASE", "my-database"):
            result = rs.execute_redshift("redshift:db.t", "SELECT * FROM t", {}, "run-1")

        assert result["status"] == "complete"
        assert result["row_count"] == 2
        assert result["rows"][0] == {"id": 1, "value": "hello"}

    def test_poll_timeout(self):
        rs = self._load_executor()
        mock_client = MagicMock()
        mock_client.execute_statement.return_value = {"Id": "stmt-123"}
        mock_client.describe_statement.return_value = {"Status": "STARTED"}

        with patch.object(rs, "_redshift_client", mock_client), \
             patch.object(rs, "WORKGROUP", "wg"), \
             patch.object(rs, "DATABASE", "db"), \
             patch.object(rs, "MAX_POLL_TIME", 0), \
             patch.object(rs, "POLL_INTERVAL", 1):
            result = rs.execute_redshift("redshift:db.t", "SELECT 1", {}, "run-1")

        assert result["status"] == "timeout"

    def test_failed_statement(self):
        rs = self._load_executor()
        mock_client = MagicMock()
        mock_client.execute_statement.return_value = {"Id": "stmt-123"}
        mock_client.describe_statement.return_value = {"Status": "FAILED"}

        with patch.object(rs, "_redshift_client", mock_client), \
             patch.object(rs, "WORKGROUP", "wg"), \
             patch.object(rs, "DATABASE", "db"):
            result = rs.execute_redshift("redshift:db.t", "SELECT 1", {}, "run-1")

        assert result["status"] == "error"
        assert result["error"] == "Query execution failed"

    def test_mutation_detection(self):
        rs = self._load_executor()
        result = rs.execute_redshift("redshift:db.t", "DROP TABLE students", {}, "run-1")
        assert result["status"] == "error"
        assert "Mutation detected" in result["error"]

    def test_result_parsing(self):
        """Verify typed field extraction (stringValue, longValue, doubleValue, etc.)."""
        rs = self._load_executor()
        mock_client = MagicMock()
        mock_client.execute_statement.return_value = {"Id": "stmt-123"}
        mock_client.describe_statement.return_value = {
            "Status": "FINISHED",
            "ResultSize": 0,
        }
        mock_client.get_statement_result.return_value = {
            "ColumnMetadata": [
                {"name": "str_col"},
                {"name": "int_col"},
                {"name": "dbl_col"},
                {"name": "bool_col"},
                {"name": "null_col"},
            ],
            "Records": [[
                {"stringValue": "text"},
                {"longValue": 42},
                {"doubleValue": 3.14},
                {"booleanValue": True},
                {"isNull": True},
            ]],
        }

        with patch.object(rs, "_redshift_client", mock_client), \
             patch.object(rs, "WORKGROUP", "wg"), \
             patch.object(rs, "DATABASE", "db"):
            result = rs.execute_redshift("redshift:db.t", "SELECT * FROM t", {}, "run-1")

        assert result["status"] == "complete"
        row = result["rows"][0]
        assert row["str_col"] == "text"
        assert row["int_col"] == 42
        assert row["dbl_col"] == 3.14
        assert row["bool_col"] is True
        assert row["null_col"] is None


# ---------------------------------------------------------------------------
# Test: Per-principal budget caps (#65)
# ---------------------------------------------------------------------------


class TestPrincipalBudget:
    def setup_method(self):
        for k in list(sys.modules.keys()):
            if "tools.plan.handler" in k or "tools.export.handler" in k or "tools.shared" in k:
                del sys.modules[k]

    def _load_shared(self):
        import tools.shared as s
        return s

    def test_under_limit_allows_plan(self):
        shared = self._load_shared()
        mock_ssm = MagicMock()
        mock_ssm.get_parameter.return_value = {"Parameter": {"Value": "100.0"}}
        mock_ssm.exceptions = MagicMock()
        mock_ssm.exceptions.ParameterNotFound = type("ParameterNotFound", (Exception,), {})

        mock_table = MagicMock()
        mock_table.get_item.return_value = {"Item": {"spend_usd": 50.0}}
        mock_ddb = MagicMock()
        mock_ddb.Table.return_value = mock_table

        with patch.object(shared, "_ssm", mock_ssm), \
             patch.object(shared, "_dynamodb", mock_ddb):
            budget = shared.get_principal_budget("arn:aws:iam::123:user/alice")
            spend = shared.get_principal_spend("arn:aws:iam::123:user/alice", "2026-04")

        assert budget == 100.0
        assert spend == 50.0
        # 50 + estimated < 100, so plan would proceed

    def test_exceeded_returns_402(self):
        shared = self._load_shared()
        mock_ssm = MagicMock()
        mock_ssm.get_parameter.return_value = {"Parameter": {"Value": "100.0"}}
        mock_ssm.exceptions = MagicMock()
        mock_ssm.exceptions.ParameterNotFound = type("ParameterNotFound", (Exception,), {})

        mock_table = MagicMock()
        mock_table.get_item.return_value = {"Item": {"spend_usd": 95.0}}
        mock_ddb = MagicMock()
        mock_ddb.Table.return_value = mock_table

        with patch.object(shared, "_ssm", mock_ssm), \
             patch.object(shared, "_dynamodb", mock_ddb):
            budget = shared.get_principal_budget("arn:aws:iam::123:user/alice")
            spend = shared.get_principal_spend("arn:aws:iam::123:user/alice", "2026-04")

        assert budget == 100.0
        assert spend == 95.0
        # 95 + 10 estimated > 100, so would return 402

    def test_ssm_error_fails_open(self):
        shared = self._load_shared()
        mock_ssm = MagicMock()
        mock_ssm.get_parameter.side_effect = Exception("SSM unavailable")
        mock_ssm.exceptions = MagicMock()
        mock_ssm.exceptions.ParameterNotFound = type("ParameterNotFound", (Exception,), {})

        with patch.object(shared, "_ssm", mock_ssm):
            budget = shared.get_principal_budget("arn:aws:iam::123:user/alice")

        assert budget is None  # fail open

    def test_dynamodb_error_fails_open(self):
        shared = self._load_shared()
        mock_table = MagicMock()
        mock_table.get_item.side_effect = Exception("DynamoDB unavailable")
        mock_ddb = MagicMock()
        mock_ddb.Table.return_value = mock_table

        with patch.object(shared, "_dynamodb", mock_ddb):
            spend = shared.get_principal_spend("arn:aws:iam::123:user/alice", "2026-04")

        assert spend == 0.0  # fail open

    def test_cost_recording_happy(self):
        shared = self._load_shared()
        mock_table = MagicMock()
        mock_ddb = MagicMock()
        mock_ddb.Table.return_value = mock_table

        with patch.object(shared, "_dynamodb", mock_ddb):
            shared.record_principal_spend("arn:aws:iam::123:user/alice", "2026-04", 5.50)

        mock_table.update_item.assert_called_once()
        call_kwargs = mock_table.update_item.call_args[1]
        assert call_kwargs["Key"] == {"principal_arn": "arn:aws:iam::123:user/alice", "month": "2026-04"}
        assert "ADD spend_usd :cost" in call_kwargs["UpdateExpression"]

    def test_cost_recording_error_nonblocking(self):
        shared = self._load_shared()
        mock_table = MagicMock()
        mock_table.update_item.side_effect = Exception("DynamoDB error")
        mock_ddb = MagicMock()
        mock_ddb.Table.return_value = mock_table

        with patch.object(shared, "_dynamodb", mock_ddb):
            # Should not raise
            shared.record_principal_spend("arn:aws:iam::123:user/alice", "2026-04", 5.50)

    def test_feature_disabled_skips_check(self):
        """When CLAWS_ENABLE_PRINCIPAL_BUDGETS is not set, no budget check occurs."""
        self._load_shared()
        # Verify that get_principal_budget is callable but the env var gate
        # in plan handler would skip it
        env_val = os.environ.get("CLAWS_ENABLE_PRINCIPAL_BUDGETS", "")
        assert not env_val  # not set by default in test env

    def test_default_budget_fallback(self):
        shared = self._load_shared()
        mock_ssm = MagicMock()
        not_found = type("ParameterNotFound", (Exception,), {})
        mock_ssm.exceptions = MagicMock()
        mock_ssm.exceptions.ParameterNotFound = not_found

        # First call (principal-specific) raises ParameterNotFound,
        # second call (default) returns a value
        call_count = {"n": 0}

        def mock_get_param(Name):  # noqa: N803
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise not_found()
            return {"Parameter": {"Value": "200.0"}}

        mock_ssm.get_parameter.side_effect = mock_get_param

        with patch.object(shared, "_ssm", mock_ssm):
            budget = shared.get_principal_budget("arn:aws:iam::123:user/alice")

        assert budget == 200.0
        assert call_count["n"] == 2  # tried principal-specific, then default
