"""
E2E tests for claws-probe.

Probes the E2E Glue table created in the conftest. The probe result
is session-scoped so schema caching only happens once per test run.
"""

import pytest

from tools.tests.e2e.conftest import _E2E_SOURCE_ID, invoke

pytestmark = pytest.mark.e2e


class TestProbeE2E:
    def test_probe_missing_source_id_returns_error(self, lam):
        """probe without source_id returns an error."""
        result = invoke(lam, "claws-probe", {})
        assert "error" in result, f"Expected error for missing source_id: {result}"

    def test_probe_bad_source_id_format_returns_error(self, lam):
        """probe with malformed source_id (no colon) returns an error."""
        result = invoke(lam, "claws-probe", {"source_id": "no-colon-here"})
        assert "error" in result, f"Expected error for bad format: {result}"

    def test_probe_unsupported_backend_returns_error(self, lam):
        """probe with an unsupported backend returns an error."""
        result = invoke(lam, "claws-probe", {
            "source_id": "dynamodb:claws-plans",
        })
        assert "error" in result, f"Expected error for unsupported backend: {result}"

    def test_probe_unknown_athena_table_returns_schema_error(self, lam):
        """probe for a nonexistent Athena table returns an error in the result."""
        result = invoke(lam, "claws-probe", {
            "source_id": "athena:nonexistent_db_xyz.nonexistent_table_xyz",
        })
        # probe returns success shape but with error nested in schema or top-level error
        has_error = "error" in result or result.get("error")
        assert has_error, f"Expected error for nonexistent table: {result}"

    def test_probe_returns_schema_for_e2e_table(self, lam, glue_table, probe_result):
        """probe returns schema for the E2E Glue table."""
        assert "schema" in probe_result, f"Missing schema in probe result: {probe_result}"

    def test_probe_schema_has_expected_columns(self, lam, glue_table, probe_result):
        """The probed schema includes the id, name, and value columns."""
        schema = probe_result["schema"]
        columns = schema.get("columns", [])
        col_names = {c["name"] for c in columns}
        assert "id" in col_names, f"Missing 'id' column: {col_names}"
        assert "name" in col_names, f"Missing 'name' column: {col_names}"
        assert "value" in col_names, f"Missing 'value' column: {col_names}"

    def test_probe_schema_has_column_types(self, lam, glue_table, probe_result):
        """Each column in the schema has a type field."""
        columns = probe_result["schema"].get("columns", [])
        for col in columns:
            assert "type" in col, f"Column missing type: {col}"

    def test_probe_schema_has_source_id(self, lam, glue_table, probe_result):
        """probe response includes source_id."""
        assert probe_result.get("source_id") == _E2E_SOURCE_ID, \
            f"source_id mismatch: {probe_result.get('source_id')}"

    def test_probe_schema_has_location(self, lam, glue_table, probe_result):
        """probe schema includes the S3 storage location."""
        location = probe_result["schema"].get("location", "")
        assert location.startswith("s3://"), f"Unexpected location: {location}"

    def test_probe_schema_only_mode_returns_no_samples(self, lam, glue_table):
        """probe in schema_only mode does not return sample rows."""
        result = invoke(lam, "claws-probe", {
            "source_id": _E2E_SOURCE_ID,
            "mode": "schema_only",
        })
        assert "schema" in result, f"Missing schema: {result}"
        assert "samples" not in result or result.get("samples") == [], \
            f"Unexpected samples in schema_only mode: {result.get('samples')}"

    def test_probe_caches_schema_in_dynamodb(self, lam, glue_table, probe_result, schemas_table):
        """probe writes the schema to the claws-schemas DynamoDB table."""
        resp = schemas_table.get_item(Key={"source_id": _E2E_SOURCE_ID})
        item = resp.get("Item")
        assert item is not None, f"Schema not cached in DynamoDB for {_E2E_SOURCE_ID}"
        assert "columns" in item or "schema" in item, \
            f"Cached item missing schema data: {item}"
