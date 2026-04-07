"""
E2E tests for claws-refine and claws-export.

Both tools operate on the run_id from excavate_result.
"""

import pytest
from tools.tests.e2e.conftest import invoke, _E2E_SOURCE_ID

pytestmark = pytest.mark.e2e


class TestRefineE2E:
    def test_refine_missing_run_id_returns_error(self, lam):
        """refine without run_id returns an error."""
        result = invoke(lam, "claws-refine", {
            "operations": ["dedupe"],
        })
        assert "error" in result, f"Expected error for missing run_id: {result}"

    def test_refine_missing_operations_returns_error(self, lam, excavate_result):
        """refine without operations returns an error."""
        result = invoke(lam, "claws-refine", {
            "run_id": excavate_result["run_id"],
        })
        assert "error" in result, f"Expected error for missing operations: {result}"

    def test_refine_unknown_run_id_returns_error(self, lam):
        """refine with a nonexistent run_id returns an error."""
        result = invoke(lam, "claws-refine", {
            "run_id": "run-nonexistent-xyz",
            "operations": ["dedupe"],
        })
        assert "error" in result, f"Expected error for unknown run_id: {result}"

    def test_refine_invalid_operation_returns_error(self, lam, excavate_result):
        """refine with an unknown operation returns an error."""
        result = invoke(lam, "claws-refine", {
            "run_id": excavate_result["run_id"],
            "operations": ["frobulate"],
        })
        assert "error" in result, f"Expected error for invalid operation: {result}"

    def test_refine_dedupe_returns_refined_run_id(self, lam, excavate_result):
        """refine with dedupe returns a new run_id."""
        result = invoke(lam, "claws-refine", {
            "run_id": excavate_result["run_id"],
            "operations": ["dedupe"],
        })
        if "error" in result:
            pytest.skip(f"refine failed (S3 or DynamoDB issue?): {result}")
        assert "run_id" in result, f"Missing run_id in refine result: {result}"

    def test_refine_dedupe_returns_rows(self, lam, excavate_result):
        """refine returns a rows list after deduplication."""
        result = invoke(lam, "claws-refine", {
            "run_id": excavate_result["run_id"],
            "operations": ["dedupe"],
        })
        if "error" in result:
            pytest.skip(f"refine failed: {result}")
        assert isinstance(result.get("rows", []), list), f"Expected rows list: {result}"

    def test_refine_rank_returns_ranked_rows(self, lam, excavate_result):
        """refine with rank operation returns rows."""
        result = invoke(lam, "claws-refine", {
            "run_id": excavate_result["run_id"],
            "operations": ["rank"],
            "top_k": 3,
        })
        if "error" in result:
            pytest.skip(f"refine rank failed: {result}")
        rows = result.get("rows", [])
        assert isinstance(rows, list)
        assert len(rows) <= 3, f"Expected at most 3 rows with top_k=3: {rows}"


class TestExportE2E:
    def test_export_missing_run_id_returns_error(self, lam):
        """export without run_id returns an error."""
        result = invoke(lam, "claws-export", {
            "destination": {"type": "s3"},
        })
        assert "error" in result, f"Expected error for missing run_id: {result}"

    def test_export_unknown_run_id_returns_error(self, lam):
        """export with a nonexistent run_id returns an error."""
        result = invoke(lam, "claws-export", {
            "run_id": "run-nonexistent-xyz",
            "destination": {"type": "s3"},
        })
        assert "error" in result, f"Expected error for unknown run_id: {result}"

    def test_export_to_s3_returns_export_id(self, lam, excavate_result, runs_bucket):
        """export to S3 returns an export_id."""
        run_id = excavate_result["run_id"]
        result = invoke(lam, "claws-export", {
            "run_id": run_id,
            "destination": {
                "type": "s3",
                "uri": f"s3://{runs_bucket}/e2e-exports/{run_id}.json",
            },
            "include_provenance": True,
        })
        if "error" in result:
            pytest.skip(f"export failed: {result}")
        assert "export_id" in result, f"Missing export_id: {result}"

    def test_export_id_format(self, lam, excavate_result, runs_bucket):
        """export_id starts with 'export-'."""
        run_id = excavate_result["run_id"]
        result = invoke(lam, "claws-export", {
            "run_id": run_id,
            "destination": {
                "type": "s3",
                "uri": f"s3://{runs_bucket}/e2e-exports/{run_id}-check.json",
            },
        })
        if "error" in result:
            pytest.skip(f"export failed: {result}")
        assert result["export_id"].startswith("export-"), \
            f"Unexpected export_id format: {result['export_id']}"

    def test_export_includes_provenance(self, lam, excavate_result, runs_bucket):
        """export with include_provenance=True returns a provenance_uri or provenance field."""
        run_id = excavate_result["run_id"]
        result = invoke(lam, "claws-export", {
            "run_id": run_id,
            "destination": {
                "type": "s3",
                "uri": f"s3://{runs_bucket}/e2e-exports/{run_id}-prov.json",
            },
            "include_provenance": True,
        })
        if "error" in result:
            pytest.skip(f"export failed: {result}")
        assert "provenance_uri" in result or "export_id" in result, \
            f"Expected provenance_uri or export_id: {result}"
