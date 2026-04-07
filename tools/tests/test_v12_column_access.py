"""v0.12.0 tests — issues #61, #62, #73, #74.

Covers:
- Multi-backend cost estimator: DynamoDB and MCP backends (#62)
- HMAC-SHA-256 in audit_export: keyed hash is non-invertible (#73)
- MCP plan validation: unregistered server rejected at plan time (#74)
- Column-level access control: schema filtering and excavate post-filter (#61)
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from tools.audit_export.handler import _hmac_sha256_of, _sanitise_record
from tools.plan.validators.cost_estimator import _estimate_dynamodb, _estimate_mcp, estimate_cost
from tools.shared import cache_schema, store_plan

# ---------------------------------------------------------------------------
# Issue #62 — Multi-backend cost estimator: DynamoDB and MCP
# ---------------------------------------------------------------------------


class TestDynamoDBCostEstimator:
    def test_basic_estimate_uses_row_count_and_avg_bytes(self):
        schema = {"row_count_estimate": 10_000, "avg_item_size_bytes": 512}
        result = _estimate_dynamodb("SELECT * FROM table", schema)
        assert result["estimated_bytes_scanned"] == 10_000 * 512
        assert result["estimated_cost_dollars"] > 0
        assert result["confidence"] == "low"
        assert "RRU" in result["notes"]

    def test_defaults_when_schema_missing_size_info(self):
        result = _estimate_dynamodb("SELECT * FROM table", {})
        # Defaults: 10,000 rows * 512 bytes = 5,120,000 bytes
        assert result["estimated_bytes_scanned"] == 10_000 * 512
        assert result["estimated_cost_dollars"] > 0

    def test_estimate_cost_dispatches_dynamodb(self):
        schema = {"row_count_estimate": 100, "avg_item_size_bytes": 256}
        result = estimate_cost("dynamodb:my-table", "SELECT * FROM my-table", schema)
        assert result["estimated_cost_dollars"] >= 0
        assert result["confidence"] == "low"

    def test_cost_scales_with_table_size(self):
        small = _estimate_dynamodb("q", {"row_count_estimate": 100, "avg_item_size_bytes": 512})
        large = _estimate_dynamodb("q", {"row_count_estimate": 1_000_000, "avg_item_size_bytes": 512})
        assert large["estimated_cost_dollars"] > small["estimated_cost_dollars"]
        assert large["estimated_bytes_scanned"] > small["estimated_bytes_scanned"]


class TestMCPCostEstimator:
    def test_mcp_cost_is_zero(self):
        result = _estimate_mcp("{}", {})
        assert result["estimated_cost_dollars"] == 0.0
        assert result["estimated_bytes_scanned"] == 0
        assert result["confidence"] == "high"

    def test_estimate_cost_dispatches_mcp(self):
        result = estimate_cost("mcp://server/resource", "{}", {})
        assert result["estimated_cost_dollars"] == 0.0
        assert result["confidence"] == "high"

    def test_mcp_notes_mention_external_pricing(self):
        result = _estimate_mcp("{}", {})
        assert "MCP" in result["notes"] or "mcp" in result["notes"].lower()


# ---------------------------------------------------------------------------
# Issue #73 — HMAC-SHA-256 in audit_export
# ---------------------------------------------------------------------------


class TestHmacAuditHash:
    def test_hmac_differs_from_plain_sha256(self):
        """HMAC output must not match plain SHA-256 when a key is configured."""
        import hashlib

        obj = {"objective": "SELECT * FROM patients", "source_id": "athena:db.patients"}
        canonical = json.dumps(obj, sort_keys=True, default=str).encode("utf-8")
        plain_hash = hashlib.sha256(canonical).hexdigest()

        with patch(
            "tools.audit_export.handler._get_hmac_key",
            return_value=b"test-key-for-unit-tests",
        ):
            hmac_hash = _hmac_sha256_of(obj)

        assert hmac_hash != plain_hash, "HMAC must differ from plain SHA-256"

    def test_same_inputs_same_hmac(self):
        """HMAC is deterministic: same inputs + same key = same digest."""
        obj = {"tool": "excavate", "principal": "user-123"}
        with patch(
            "tools.audit_export.handler._get_hmac_key",
            return_value=b"stable-key",
        ):
            h1 = _hmac_sha256_of(obj)
            h2 = _hmac_sha256_of(obj)
        assert h1 == h2

    def test_different_keys_produce_different_hashes(self):
        """Different deployment keys produce different digests for the same input."""
        obj = {"tool": "plan", "source_id": "athena:db.t"}
        with patch(
            "tools.audit_export.handler._get_hmac_key", return_value=b"key-a"
        ):
            h_a = _hmac_sha256_of(obj)
        with patch(
            "tools.audit_export.handler._get_hmac_key", return_value=b"key-b"
        ):
            h_b = _hmac_sha256_of(obj)
        assert h_a != h_b

    def test_falls_back_to_sha256_when_no_key(self):
        """When AUDIT_HMAC_KEY_ARN is unset, _get_hmac_key returns b'' and
        _hmac_sha256_of falls back to plain SHA-256 without raising."""
        import hashlib

        obj = {"tool": "probe"}
        canonical = json.dumps(obj, sort_keys=True, default=str).encode("utf-8")
        expected = hashlib.sha256(canonical).hexdigest()

        with patch("tools.audit_export.handler._get_hmac_key", return_value=b""):
            result = _hmac_sha256_of(obj)

        assert result == expected

    def test_sanitise_record_uses_hmac(self):
        """_sanitise_record calls _hmac_sha256_of for both inputs and outputs."""
        record = {
            "tool": "excavate",
            "principal": "user-abc",
            "inputs": {"query": "SELECT ssn FROM patients LIMIT 5"},
            "outputs": {"rows_returned": 5},
            "cost": 0.01,
            "guardrail_trace": None,
            "timestamp": "2026-04-06T12:00:00Z",
        }
        with patch(
            "tools.audit_export.handler._get_hmac_key",
            return_value=b"test-secret",
        ):
            sanitised = _sanitise_record(record)

        assert "inputs_hash" in sanitised
        assert "outputs_hash" in sanitised
        assert "ssn" not in json.dumps(sanitised)
        assert "patients" not in json.dumps(sanitised)


# ---------------------------------------------------------------------------
# Issue #74 — MCP source_id validation in plan handler
# ---------------------------------------------------------------------------


class TestMCPPlanValidation:
    @pytest.fixture(autouse=True)
    def _patch_registry(self):
        """Provide a controlled registry with one known server."""
        with patch(
            "tools.mcp.registry.known_servers",
            return_value={"postgres-prod"},
        ):
            yield

    def _make_plan_event(self, source_id: str) -> dict:
        return {
            "objective": "count rows",
            "source_id": source_id,
            "requestContext": {
                "authorizer": {"principalId": "user-1", "roles": "[]"},
                "requestId": "req-001",
            },
        }

    def test_plan_rejects_unregistered_mcp_server(self, plans_table, schemas_table):
        """Plan handler returns error for an MCP source_id not in the registry.

        MCP validation runs before schema lookup, so no cached schema is required.
        """
        from tools.plan.handler import handler

        event = self._make_plan_event("mcp://unknown-server/data")
        resp = handler(event, MagicMock())
        body = json.loads(resp["body"])
        assert resp["statusCode"] == 422
        assert "unknown-server" in body.get("error", "")

    def test_plan_allows_registered_mcp_server(self, plans_table, schemas_table):
        """Plan handler proceeds past validation for a registered MCP server.

        The handler will fail later (LLM / model invocation), but it must not
        reject the request at the MCP source_id validation step.
        """
        from tools.plan.handler import handler

        cache_schema(
            "mcp://postgres-prod/public",
            {
                "server": "postgres-prod",
                "resource": "public",
                "available_tools": [
                    {"name": "query", "description": "Run SQL", "input_schema": {"type": "object"}}
                ],
            },
        )
        with patch("tools.plan.handler.call_router", return_value=None), \
             patch("tools.plan.handler.bedrock_runtime") as mock_br:
            # Simulate model returning a valid plan JSON
            mock_response = MagicMock()
            mock_response["body"].read.return_value = json.dumps({
                "content": [{"type": "text", "text": json.dumps({
                    "query": '{"server": "postgres-prod", "tool": "query", "arguments": {}}',
                    "output_schema": {"columns": ["count"], "estimated_rows": 1},
                    "reasoning": "simple count",
                })}]
            }).encode()
            mock_br.return_value.invoke_model.return_value = mock_response

            event = self._make_plan_event("mcp://postgres-prod/public")
            resp = handler(event, MagicMock())

        # Must not be a 422 MCP validation error
        assert resp["statusCode"] != 422 or "not registered" not in resp.get("body", "")


# ---------------------------------------------------------------------------
# Issue #61 — Column-level access control
# ---------------------------------------------------------------------------


SCHEMA_WITH_RESTRICTIONS = {
    "database": "clinical",
    "table": "patients",
    "columns": [
        {"name": "patient_id", "type": "string", "visibility": "public"},
        {"name": "diagnosis_code", "type": "string", "visibility": "restricted"},
        {"name": "ssn", "type": "string", "visibility": "phi"},
        {"name": "age", "type": "int", "visibility": "public"},
    ],
    "size_bytes_estimate": 50_000_000,
}

SCHEMA_ALL_PUBLIC = {
    "database": "analytics",
    "table": "enrollments",
    "columns": [
        {"name": "student_id", "type": "string", "visibility": "public"},
        {"name": "gpa", "type": "double", "visibility": "public"},
    ],
    "size_bytes_estimate": 10_000_000,
}


class TestSchemaColumnFiltering:
    """Unit tests for _filter_schema_columns (no AWS needed)."""

    def test_no_filtering_when_all_public(self):
        from tools.plan.handler import _filter_schema_columns
        schema, allowed = _filter_schema_columns(SCHEMA_ALL_PUBLIC, [])
        assert allowed is None  # None means "no restrictions, pass all through"
        assert schema is SCHEMA_ALL_PUBLIC

    def test_basic_principal_sees_only_public_columns(self):
        from tools.plan.handler import _filter_schema_columns
        _, allowed = _filter_schema_columns(SCHEMA_WITH_RESTRICTIONS, [])
        assert "patient_id" in allowed
        assert "age" in allowed
        assert "diagnosis_code" not in allowed
        assert "ssn" not in allowed

    def test_pii_access_role_sees_restricted_but_not_phi(self):
        from tools.plan.handler import _filter_schema_columns
        _, allowed = _filter_schema_columns(SCHEMA_WITH_RESTRICTIONS, ["pii_access"])
        assert "patient_id" in allowed
        assert "diagnosis_code" in allowed  # restricted, accessible with pii_access
        assert "ssn" not in allowed         # phi, requires phi_cleared

    def test_phi_cleared_role_sees_all_columns(self):
        from tools.plan.handler import _filter_schema_columns
        _, allowed = _filter_schema_columns(SCHEMA_WITH_RESTRICTIONS, ["phi_cleared"])
        assert set(allowed) == {"patient_id", "diagnosis_code", "ssn", "age"}

    def test_phi_cleared_implies_pii_access(self):
        from tools.plan.handler import _filter_schema_columns
        # phi_cleared should grant access to restricted columns too
        _, allowed = _filter_schema_columns(SCHEMA_WITH_RESTRICTIONS, ["phi_cleared"])
        assert "diagnosis_code" in allowed

    def test_empty_schema_columns_returns_none_allowed(self):
        from tools.plan.handler import _filter_schema_columns
        _, allowed = _filter_schema_columns({"columns": []}, [])
        assert allowed is None


class TestExcavateColumnPostFilter:
    """Test that excavate post-filters result rows to allowed_columns."""

    @pytest.fixture(autouse=True)
    def resources(self, s3_bucket, plans_table, schemas_table):
        yield

    def _make_mock_executor(self, rows: list[dict]) -> MagicMock:
        """Return a MagicMock executor that returns the given rows."""
        mock = MagicMock()
        mock.return_value = {
            "status": "complete",
            "rows": rows,
            "bytes_scanned": 10_000,
            "cost": "$0.00",
        }
        return mock

    def test_restricted_columns_stripped_from_results(self, s3_bucket, plans_table):
        """Excavate removes columns not in plan.allowed_columns before storing results."""
        import tools.excavate.handler as _excavate

        store_plan("plan-col001", {
            "source_id": "athena:clinical.patients",
            "query": "SELECT patient_id, age FROM clinical.patients LIMIT 5",
            "query_type": "athena_sql",
            "created_by": "researcher1",
            "status": "ready",
            "allowed_columns": ["patient_id", "age"],  # ssn and diagnosis_code excluded
        })

        # Executor returns rows that include a restricted column (simulating LLM leak)
        leaked_rows = [
            {"patient_id": "P001", "age": 34, "ssn": "123-45-6789", "diagnosis_code": "E11.9"},
            {"patient_id": "P002", "age": 52, "ssn": "987-65-4321", "diagnosis_code": "I10"},
        ]
        mock_executor = self._make_mock_executor(leaked_rows)

        with patch.dict(_excavate.EXECUTORS, {"athena_sql": mock_executor}), \
             patch("tools.excavate.handler.scan_payload", return_value={"status": "ok"}):
            resp = _excavate.handler({
                "plan_id": "plan-col001",
                "source_id": "athena:clinical.patients",
                "query": "SELECT patient_id, age FROM clinical.patients LIMIT 5",
                "query_type": "athena_sql",
                "constraints": {},
                "requestContext": {
                    "authorizer": {"principalId": "researcher1"},
                    "requestId": "req-col001",
                },
            }, MagicMock())

        body = json.loads(resp["body"])
        assert resp["statusCode"] == 200
        assert body["rows_returned"] == 2
        # Preview rows should only contain allowed columns
        for row in body.get("result_preview", []):
            assert "ssn" not in row, "SSN must be stripped by column post-filter"
            assert "diagnosis_code" not in row, "restricted column must be stripped"
            assert "patient_id" in row
            assert "age" in row

    def test_no_filtering_when_allowed_columns_absent(self, s3_bucket, plans_table):
        """When plan has no allowed_columns, all result columns pass through."""
        import tools.excavate.handler as _excavate

        store_plan("plan-col002", {
            "source_id": "athena:analytics.enrollments",
            "query": "SELECT * FROM analytics.enrollments LIMIT 5",
            "query_type": "athena_sql",
            "created_by": "analyst1",
            "status": "ready",
            # No allowed_columns — all public schema
        })

        rows = [{"student_id": "S001", "gpa": 3.8, "credits": 120}]
        mock_executor = self._make_mock_executor(rows)

        with patch.dict(_excavate.EXECUTORS, {"athena_sql": mock_executor}), \
             patch("tools.excavate.handler.scan_payload", return_value={"status": "ok"}):
            resp = _excavate.handler({
                "plan_id": "plan-col002",
                "source_id": "athena:analytics.enrollments",
                "query": "SELECT * FROM analytics.enrollments LIMIT 5",
                "query_type": "athena_sql",
                "constraints": {},
                "requestContext": {
                    "authorizer": {"principalId": "analyst1"},
                    "requestId": "req-col002",
                },
            }, MagicMock())

        body = json.loads(resp["body"])
        assert resp["statusCode"] == 200
        preview = body.get("result_preview", [])
        assert len(preview) == 1
        # All columns should be present — no filtering when no allowed_columns
        assert preview[0].get("credits") == 120
