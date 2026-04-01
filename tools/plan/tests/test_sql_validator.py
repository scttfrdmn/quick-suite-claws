"""Tests for the SQL validator."""

import pytest

from tools.plan.validators.sql_validator import validate_sql


class TestValidateSQL:
    def test_valid_select(self):
        result = validate_sql(
            "SELECT cohort, COUNT(*) FROM variants GROUP BY cohort",
            {"read_only": True},
        )
        assert result["ok"] is True

    def test_valid_cte(self):
        result = validate_sql(
            "WITH top AS (SELECT * FROM t LIMIT 10) SELECT * FROM top",
            {"read_only": True},
        )
        assert result["ok"] is True

    def test_rejects_insert(self):
        result = validate_sql(
            "INSERT INTO targets SELECT * FROM variants",
            {"read_only": True},
        )
        assert result["ok"] is False
        assert "INSERT" in result["reason"]

    def test_rejects_delete(self):
        result = validate_sql(
            "DELETE FROM variants WHERE gene = 'BRCA1'",
            {"read_only": True},
        )
        assert result["ok"] is False
        assert "DELETE" in result["reason"]

    def test_rejects_drop(self):
        result = validate_sql(
            "DROP TABLE variants",
            {"read_only": True},
        )
        assert result["ok"] is False
        assert "DROP" in result["reason"]

    def test_rejects_multiple_statements(self):
        # Multi-statement with non-mutation second query
        result = validate_sql(
            "SELECT 1; SELECT 2",
            {"read_only": True},
        )
        assert result["ok"] is False
        assert "Multiple" in result["reason"]

    def test_rejects_multiple_statements_with_mutation(self):
        # Multi-statement where mutation check may fire first — either
        # rejection reason is acceptable, the key invariant is rejection
        result = validate_sql(
            "SELECT 1; DROP TABLE variants",
            {"read_only": True},
        )
        assert result["ok"] is False

    def test_allows_trailing_semicolon(self):
        result = validate_sql(
            "SELECT cohort FROM variants;",
            {"read_only": True},
        )
        assert result["ok"] is True

    def test_rejects_empty(self):
        result = validate_sql("", {"read_only": True})
        assert result["ok"] is False

    def test_rejects_update(self):
        result = validate_sql(
            "UPDATE variants SET classification = 'benign'",
            {"read_only": True},
        )
        assert result["ok"] is False

    def test_rejects_grant(self):
        result = validate_sql(
            "GRANT ALL ON variants TO PUBLIC",
            {"read_only": True},
        )
        assert result["ok"] is False

    def test_warns_on_union(self):
        result = validate_sql(
            "SELECT * FROM a UNION SELECT * FROM b",
            {"read_only": True},
        )
        assert result["ok"] is True
        assert "warnings" in result
        assert any("UNION" in w for w in result["warnings"])

    def test_rejects_non_select_start(self):
        result = validate_sql(
            "CALL some_procedure()",
            {"read_only": True},
        )
        assert result["ok"] is False
