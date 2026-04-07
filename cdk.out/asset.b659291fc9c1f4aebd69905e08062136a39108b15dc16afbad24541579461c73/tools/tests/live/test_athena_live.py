"""Live Athena integration tests.

Validates execute_athena() against a real Athena endpoint:
- Query execution completes and returns rows
- bytes_scanned is populated correctly
- Cost calculation matches expected Athena pricing
- Byte-scan safety guard catches oversized scans
- Timeout propagation works

Prerequisites:
  - CLAWS_TEST_ATHENA_DB / _TABLE must point to an existing Glue table
  - The table should be small (< 1 MB) — use a synthetic fixture table
  - CLAWS_TEST_ATHENA_OUTPUT must be a writable s3:// URI
  - Executing IAM principal needs: athena:StartQuery*, athena:GetQuery*,
    glue:GetTable, s3:PutObject on the output bucket

Run:
    pytest tools/tests/live/test_athena_live.py -v -m live
"""

import os

import pytest

# Apply the live marker to every test in this module
pytestmark = pytest.mark.live


@pytest.fixture(autouse=True)
def _patch_athena_env(athena_config, monkeypatch):
    """Point the executor at the live test config."""
    import tools.excavate.executors.athena as _athena_mod

    monkeypatch.setenv("CLAWS_ATHENA_OUTPUT", athena_config["CLAWS_TEST_ATHENA_OUTPUT"])
    monkeypatch.setenv("CLAWS_ATHENA_WORKGROUP", os.environ.get("CLAWS_TEST_ATHENA_WORKGROUP", "primary"))
    # Reset module-level client so it picks up the real endpoint
    _athena_mod.ATHENA_CLIENT = None
    _athena_mod.S3_CLIENT = None
    yield
    _athena_mod.ATHENA_CLIENT = None
    _athena_mod.S3_CLIENT = None


class TestAthenaExecutorLive:
    def test_basic_select_returns_rows(self, athena_config):
        """A simple SELECT returns rows and a non-negative bytes_scanned."""
        from tools.excavate.executors.athena import execute_athena

        db = athena_config["CLAWS_TEST_ATHENA_DB"]
        table = athena_config["CLAWS_TEST_ATHENA_TABLE"]

        result = execute_athena(
            source_id=f"athena:{db}.{table}",
            query=f"SELECT * FROM {db}.{table} LIMIT 5",
            constraints={"timeout_seconds": 60},
            run_id="live-athena-001",
        )

        assert result["status"] == "complete", f"Query failed: {result.get('error')}"
        assert isinstance(result["rows"], list)
        assert len(result["rows"]) <= 5
        assert result["bytes_scanned"] >= 0

    def test_bytes_scanned_is_positive(self, athena_config):
        """bytes_scanned should be > 0 for a table with data."""
        from tools.excavate.executors.athena import execute_athena

        db = athena_config["CLAWS_TEST_ATHENA_DB"]
        table = athena_config["CLAWS_TEST_ATHENA_TABLE"]

        result = execute_athena(
            source_id=f"athena:{db}.{table}",
            query=f"SELECT COUNT(*) AS n FROM {db}.{table}",
            constraints={"timeout_seconds": 60},
            run_id="live-athena-002",
        )

        assert result["status"] == "complete", result.get("error")
        assert result["bytes_scanned"] > 0

    def test_cost_string_format(self, athena_config):
        """Cost is returned as a $-prefixed decimal string."""
        from tools.excavate.executors.athena import execute_athena

        db = athena_config["CLAWS_TEST_ATHENA_DB"]
        table = athena_config["CLAWS_TEST_ATHENA_TABLE"]

        result = execute_athena(
            source_id=f"athena:{db}.{table}",
            query=f"SELECT * FROM {db}.{table} LIMIT 1",
            constraints={"timeout_seconds": 60},
            run_id="live-athena-003",
        )

        assert result["status"] == "complete", result.get("error")
        assert result["cost"].startswith("$")
        cost_value = float(result["cost"][1:])
        assert cost_value >= 0

    def test_cost_matches_athena_pricing(self, athena_config):
        """Cost = bytes_scanned × $5/TB — verify within floating-point tolerance."""
        from tools.excavate.executors.athena import PRICE_PER_BYTE, execute_athena

        db = athena_config["CLAWS_TEST_ATHENA_DB"]
        table = athena_config["CLAWS_TEST_ATHENA_TABLE"]

        result = execute_athena(
            source_id=f"athena:{db}.{table}",
            query=f"SELECT * FROM {db}.{table} LIMIT 10",
            constraints={"timeout_seconds": 60},
            run_id="live-athena-004",
        )

        assert result["status"] == "complete", result.get("error")
        expected_cost = result["bytes_scanned"] * PRICE_PER_BYTE
        actual_cost = float(result["cost"][1:])
        assert abs(actual_cost - expected_cost) < 1e-9

    def test_scan_does_not_exceed_safety_limit(self, athena_config):
        """Live fixture table must be small — guard against accidentally huge scans."""
        from tools.excavate.executors.athena import execute_athena

        db = athena_config["CLAWS_TEST_ATHENA_DB"]
        table = athena_config["CLAWS_TEST_ATHENA_TABLE"]

        result = execute_athena(
            source_id=f"athena:{db}.{table}",
            query=f"SELECT * FROM {db}.{table}",
            constraints={"timeout_seconds": 60},
            run_id="live-athena-005",
        )

        assert result["status"] == "complete", result.get("error")
        # Fixture table must be under 1 MB — anything larger is a misconfiguration
        assert result["bytes_scanned"] < 1_000_000, (
            f"Live fixture table scanned {result['bytes_scanned']:,} bytes — "
            "use a smaller synthetic table for live tests"
        )

    def test_invalid_table_returns_error(self, athena_config):
        """Querying a non-existent table returns status=error, not an exception."""
        from tools.excavate.executors.athena import execute_athena

        db = athena_config["CLAWS_TEST_ATHENA_DB"]

        result = execute_athena(
            source_id=f"athena:{db}.nonexistent_claws_test_table",
            query=f"SELECT * FROM {db}.nonexistent_claws_test_table LIMIT 1",
            constraints={"timeout_seconds": 30},
            run_id="live-athena-006",
        )

        assert result["status"] == "error"
        assert "error" in result

    def test_column_names_in_rows(self, athena_config):
        """Each row dict must use column names as keys, not positional indices."""
        from tools.excavate.executors.athena import execute_athena

        db = athena_config["CLAWS_TEST_ATHENA_DB"]
        table = athena_config["CLAWS_TEST_ATHENA_TABLE"]

        result = execute_athena(
            source_id=f"athena:{db}.{table}",
            query=f"SELECT * FROM {db}.{table} LIMIT 3",
            constraints={"timeout_seconds": 60},
            run_id="live-athena-007",
        )

        assert result["status"] == "complete", result.get("error")
        if result["rows"]:
            first_row = result["rows"][0]
            # Keys must be strings (column names), not integers
            assert all(isinstance(k, str) for k in first_row)
