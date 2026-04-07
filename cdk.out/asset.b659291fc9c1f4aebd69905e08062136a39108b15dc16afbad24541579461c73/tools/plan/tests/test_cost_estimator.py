"""Tests for the cost estimator."""

from tools.plan.validators.cost_estimator import estimate_cost


class TestEstimateCost:
    def test_athena_full_scan(self):
        schema = {"size_bytes_estimate": 1_000_000_000}  # 1 GB
        result = estimate_cost(
            "athena:db.table",
            "SELECT * FROM table",
            schema,
        )
        assert result["estimated_bytes_scanned"] == 1_000_000_000
        assert result["estimated_cost_dollars"] > 0
        assert result["confidence"] in ("low", "medium", "high")

    def test_athena_unknown_size(self):
        schema = {}
        result = estimate_cost("athena:db.table", "SELECT 1", schema)
        assert result["confidence"] == "low"
        assert result["estimated_bytes_scanned"] == 10 * 1024 * 1024  # minimum

    def test_athena_partition_pruning(self):
        schema = {
            "size_bytes_estimate": 10_000_000_000,  # 10 GB
            "columns": [
                {"name": "cohort", "type": "string", "partition_key": True},
                {"name": "gene", "type": "string"},
            ],
        }
        result = estimate_cost(
            "athena:db.table",
            "SELECT * FROM table WHERE cohort = 'TCGA'",
            schema,
        )
        # Should be significantly less than full scan
        assert result["estimated_bytes_scanned"] < 10_000_000_000

    def test_opensearch_zero_cost(self):
        result = estimate_cost("opensearch:index", "{}", {})
        assert result["estimated_cost_dollars"] == 0.0
        assert result["confidence"] == "high"
