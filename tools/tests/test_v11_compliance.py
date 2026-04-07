"""v0.11.0 compliance feature tests — issues #56–#60.

Covers:
- IRB pending_approval gate in excavate (issue #56)
- approve_plan by authorized vs unauthorized principal (issue #56)
- Excavate succeeds after approval (issue #56)
- Cedar template files presence and syntax check (issue #58)
- FERPA guardrail config file validation (issue #57)
- Audit export NDJSON format and field presence (issue #59)
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import boto3
import pytest

from tools.approve_plan.handler import handler as approve_plan_handler
from tools.audit_export.handler import _hmac_sha256_of, _sanitise_record
from tools.audit_export.handler import handler as audit_export_handler
from tools.excavate.handler import handler as excavate_handler
from tools.shared import cache_schema, store_plan

SAMPLE_QUERY = "SELECT gene, chromosome FROM genomics.variants LIMIT 10"
SAMPLE_SCHEMA = {
    "database": "genomics",
    "table": "variants",
    "columns": [{"name": "gene", "type": "string"}, {"name": "chromosome", "type": "string"}],
    "size_bytes_estimate": 1_000_000_000,
}

POLICIES_TEMPLATES_DIR = Path(__file__).parent.parent.parent / "policies" / "templates"
GUARDRAILS_FERPA_FILE = (
    Path(__file__).parent.parent.parent / "guardrails" / "ferpa" / "ferpa_guardrail.json"
)


# ---------------------------------------------------------------------------
# Issue #56 — IRB pending_approval gate in excavate
# ---------------------------------------------------------------------------

class TestPendingApprovalGate:
    @pytest.fixture(autouse=True)
    def resources(self, s3_bucket, plans_table, schemas_table):
        cache_schema("athena:genomics.variants", SAMPLE_SCHEMA)
        yield

    def test_excavate_blocked_when_pending_approval(self, s3_bucket, plans_table):
        """Excavate returns pending_approval status when plan has pending_approval status."""
        store_plan("plan-irb001", {
            "source_id": "athena:genomics.variants",
            "query": SAMPLE_QUERY,
            "query_type": "athena_sql",
            "created_by": "researcher1",
            "status": "pending_approval",
        })

        mock_exec = {
            "status": "complete",
            "rows": [{"gene": "BRCA1", "chromosome": "17"}],
            "bytes_scanned": 512,
            "cost": "$0.0000",
        }
        with patch.dict(
            "tools.excavate.handler.EXECUTORS",
            {"athena_sql": lambda **kw: mock_exec},
        ):
            resp = excavate_handler(
                {
                    "plan_id": "plan-irb001",
                    "source_id": "athena:genomics.variants",
                    "query": SAMPLE_QUERY,
                    "query_type": "athena_sql",
                    "requestContext": {"authorizer": {"principalId": "researcher1"}, "requestId": ""},
                },
                None,
            )

        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["status"] == "pending_approval"
        assert body["plan_id"] == "plan-irb001"
        assert "IRB approval" in body["message"]

    def test_excavate_succeeds_after_approval(self, s3_bucket, plans_table, monkeypatch):
        """Excavate proceeds normally after the plan is approved."""
        store_plan("plan-irb002", {
            "source_id": "athena:genomics.variants",
            "query": SAMPLE_QUERY,
            "query_type": "athena_sql",
            "created_by": "researcher1",
            "status": "approved",
            "approved_by": "irb-officer",
        })

        mock_exec = {
            "status": "complete",
            "rows": [{"gene": "TP53", "chromosome": "17"}],
            "bytes_scanned": 1024,
            "cost": "$0.0001",
        }
        with patch.dict(
            "tools.excavate.handler.EXECUTORS",
            {"athena_sql": lambda **kw: mock_exec},
        ):
            resp = excavate_handler(
                {
                    "plan_id": "plan-irb002",
                    "source_id": "athena:genomics.variants",
                    "query": SAMPLE_QUERY,
                    "query_type": "athena_sql",
                    "requestContext": {"authorizer": {"principalId": "researcher1"}, "requestId": ""},
                },
                None,
            )

        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["status"] == "complete"
        assert body["rows_returned"] == 1

    def test_excavate_ready_plan_unaffected(self, s3_bucket, plans_table):
        """Plans with status 'ready' (no IRB) are not affected by the IRB gate."""
        store_plan("plan-irb003", {
            "source_id": "athena:genomics.variants",
            "query": SAMPLE_QUERY,
            "query_type": "athena_sql",
            "created_by": "analyst",
            "status": "ready",
        })

        mock_exec = {
            "status": "complete",
            "rows": [{"gene": "EGFR", "chromosome": "7"}],
            "bytes_scanned": 256,
            "cost": "$0.0000",
        }
        with patch.dict(
            "tools.excavate.handler.EXECUTORS",
            {"athena_sql": lambda **kw: mock_exec},
        ):
            resp = excavate_handler(
                {
                    "plan_id": "plan-irb003",
                    "source_id": "athena:genomics.variants",
                    "query": SAMPLE_QUERY,
                    "query_type": "athena_sql",
                    "requestContext": {"authorizer": {"principalId": "analyst"}, "requestId": ""},
                },
                None,
            )

        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["status"] == "complete"


# ---------------------------------------------------------------------------
# Issue #56 — approve_plan permission checks
# ---------------------------------------------------------------------------

class TestApprovePlan:
    @pytest.fixture(autouse=True)
    def resources(self, plans_table):
        # Pre-populate a pending plan
        store_plan("plan-irb010", {
            "source_id": "athena:health.patients",
            "query": "SELECT patient_id FROM health.patients LIMIT 5",
            "query_type": "athena_sql",
            "created_by": "researcher2",
            "status": "pending_approval",
            "requires_irb": True,
        })
        yield

    def test_authorized_approver_sets_approved(self, plans_table, monkeypatch):
        """An authorised IRB approver can approve a pending plan."""
        monkeypatch.setenv("CLAWS_IRB_APPROVERS", "irb-officer,irb-lead")

        # Mock EventBridge to avoid real AWS calls
        mock_events = MagicMock()
        mock_events.put_events.return_value = {"FailedEntryCount": 0, "Entries": []}

        with patch("tools.approve_plan.handler.boto3") as mock_boto3:
            mock_boto3.client.return_value = mock_events
            # Allow dynamodb_resource to work via the real substrate
            import tools.approve_plan.handler as _mod
            with patch.object(_mod, "dynamodb_resource") as mock_ddb:
                real_ddb = boto3.resource("dynamodb", region_name="us-east-1")
                mock_ddb.return_value = real_ddb
                resp = approve_plan_handler(
                    {
                        "plan_id": "plan-irb010",
                        "approved_by": "irb-officer",
                        "approval_notes": "Approved for cohort study phase 2",
                    },
                    None,
                )

        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["status"] == "approved"
        assert body["approved_by"] == "irb-officer"
        assert "approved_at" in body
        assert body.get("approval_notes") == "Approved for cohort study phase 2"

        # Verify DynamoDB was updated
        ddb = boto3.resource("dynamodb", region_name="us-east-1")
        item = ddb.Table("claws-plans").get_item(Key={"plan_id": "plan-irb010"})["Item"]
        assert item["status"] == "approved"
        assert item["approved_by"] == "irb-officer"

    def test_unauthorized_principal_returns_403(self, plans_table, monkeypatch):
        """A principal not in the irb_approvers list gets a 403."""
        monkeypatch.setenv("CLAWS_IRB_APPROVERS", "irb-officer,irb-lead")

        resp = approve_plan_handler(
            {
                "plan_id": "plan-irb010",
                "approved_by": "random-analyst",
            },
            None,
        )

        assert resp["statusCode"] == 403
        body = json.loads(resp["body"])
        assert "not authorized" in body["error"].lower() or "irb_approver" in body["error"].lower()

    def test_self_approval_returns_403(self, plans_table, monkeypatch):
        """An approver cannot approve their own plan."""
        monkeypatch.setenv("CLAWS_IRB_APPROVERS", "researcher2")

        # researcher2 is both the plan owner and listed as an approver
        resp = approve_plan_handler(
            {
                "plan_id": "plan-irb010",
                "approved_by": "researcher2",
            },
            None,
        )

        assert resp["statusCode"] == 403
        body = json.loads(resp["body"])
        assert "own plan" in body["error"].lower()

    def test_approve_plan_missing_plan_id(self, plans_table):
        """approve_plan returns 400 when plan_id is missing."""
        resp = approve_plan_handler({"approved_by": "irb-officer"}, None)
        assert resp["statusCode"] == 400

    def test_approve_plan_not_found(self, plans_table, monkeypatch):
        """approve_plan returns 404 when plan_id does not exist."""
        monkeypatch.setenv("CLAWS_IRB_APPROVERS", "irb-officer")
        resp = approve_plan_handler(
            {"plan_id": "plan-nonexistent-irb", "approved_by": "irb-officer"},
            None,
        )
        assert resp["statusCode"] == 404


# ---------------------------------------------------------------------------
# Issue #58 — Cedar policy template presence and syntax
# ---------------------------------------------------------------------------

class TestCedarTemplates:
    EXPECTED_TEMPLATES = [
        "read-only.cedar",
        "no-pii-export.cedar",
        "approved-domains-only.cedar",
        "phi-approved.cedar",
    ]

    def test_all_template_files_exist(self):
        """All four Cedar policy template files are present."""
        for template in self.EXPECTED_TEMPLATES:
            path = POLICIES_TEMPLATES_DIR / template
            assert path.exists(), f"Missing Cedar template: {template}"

    def test_templates_are_non_empty(self):
        """Each Cedar template file contains at least one permit or forbid rule."""
        for template in self.EXPECTED_TEMPLATES:
            path = POLICIES_TEMPLATES_DIR / template
            content = path.read_text()
            assert "permit(" in content or "forbid(" in content, (
                f"Template {template} contains no permit/forbid rules"
            )

    def test_read_only_blocks_excavate(self):
        """read-only template contains an explicit forbid for excavate."""
        content = (POLICIES_TEMPLATES_DIR / "read-only.cedar").read_text()
        assert 'action == Action::"excavate"' in content
        assert "forbid(" in content

    def test_no_pii_export_blocks_pii_tagged_results(self):
        """no-pii-export template has a forbid that checks pii_tags."""
        content = (POLICIES_TEMPLATES_DIR / "no-pii-export.cedar").read_text()
        assert "pii_tags" in content
        assert "forbid(" in content

    def test_approved_domains_only_checks_domain(self):
        """approved-domains-only template checks resource.domain."""
        content = (POLICIES_TEMPLATES_DIR / "approved-domains-only.cedar").read_text()
        assert "resource.domain" in content or "approved_domains" in content

    def test_phi_approved_requires_irb_approver_role(self):
        """phi-approved template checks for irb_approver role on plan.approve."""
        content = (POLICIES_TEMPLATES_DIR / "phi-approved.cedar").read_text()
        assert "irb_approver" in content
        assert 'plan.approve' in content


# ---------------------------------------------------------------------------
# Issue #57 — FERPA guardrail config file validation
# ---------------------------------------------------------------------------

class TestFerpaGuardrailConfig:
    def test_ferpa_guardrail_file_exists(self):
        """FERPA guardrail JSON file exists at the expected path."""
        assert GUARDRAILS_FERPA_FILE.exists(), (
            f"FERPA guardrail config not found: {GUARDRAILS_FERPA_FILE}"
        )

    def test_ferpa_guardrail_is_valid_json(self):
        """FERPA guardrail config is valid JSON."""
        content = GUARDRAILS_FERPA_FILE.read_text()
        config = json.loads(content)  # raises if invalid
        assert isinstance(config, dict)

    def test_ferpa_guardrail_has_required_denied_topics(self):
        """FERPA config blocks all five required topic categories."""
        config = json.loads(GUARDRAILS_FERPA_FILE.read_text())
        topic_policy = config.get("topicPolicy", {})
        topics = topic_policy.get("topics", [])
        topic_names = {t["name"] for t in topics}
        required_topics = {
            "grades",
            "enrollment_status",
            "financial_aid",
            "disciplinary_records",
            "student_schedules",
        }
        assert required_topics.issubset(topic_names), (
            f"Missing FERPA topics: {required_topics - topic_names}"
        )
        # All topics must be DENY type
        for topic in topics:
            if topic["name"] in required_topics:
                assert topic["type"] == "DENY", (
                    f"Topic '{topic['name']}' should be DENY, got {topic['type']}"
                )

    def test_ferpa_guardrail_has_pii_regex_patterns(self):
        """FERPA config includes SSN and student ID regex patterns."""
        config = json.loads(GUARDRAILS_FERPA_FILE.read_text())
        pii_config = config.get("sensitiveInformationPolicy", {})
        regexes = pii_config.get("regexes", [])
        regex_names = {r["name"] for r in regexes}
        assert "ssn_pattern" in regex_names
        assert "student_id_pattern" in regex_names

        ssn = next(r for r in regexes if r["name"] == "ssn_pattern")
        student = next(r for r in regexes if r["name"] == "student_id_pattern")
        # Verify the patterns match the spec
        import re
        assert re.search(ssn["pattern"], "123-45-6789")
        assert re.search(student["pattern"], "A1234567")
        assert not re.search(student["pattern"], "12345678")  # no letter prefix


# ---------------------------------------------------------------------------
# Issue #59 — Audit export NDJSON format and field presence
# ---------------------------------------------------------------------------

class TestAuditExport:
    def test_sanitise_record_produces_required_fields(self):
        """_sanitise_record returns all required compliance export fields."""
        raw = {
            "timestamp": "2026-04-01T12:00:00+00:00",
            "tool": "excavate",
            "principal": "user-abc",
            "request_id": "req-123",
            "inputs": {"plan_id": "plan-xyz", "source_id": "athena:db.table"},
            "outputs": {"status": "complete", "rows_returned": 42},
            "cost": 0.0012,
            "guardrail_trace": None,
        }
        record = _sanitise_record(raw)
        assert set(record.keys()) == {
            "principal", "tool", "inputs_hash", "outputs_hash",
            "cost_usd", "guardrail_trace", "timestamp",
        }

    def test_sanitise_record_hashes_inputs_and_outputs(self):
        """inputs and outputs are replaced with SHA-256 hashes, not raw values."""
        raw = {
            "timestamp": "2026-04-01T12:00:00+00:00",
            "tool": "plan",
            "principal": "user-xyz",
            "inputs": {"objective": "find all SSNs", "source_id": "athena:pii.table"},
            "outputs": {"status": "blocked"},
            "cost": None,
            "guardrail_trace": {"action": "INTERVENED"},
        }
        record = _sanitise_record(raw)
        # Verify inputs_hash is a hex SHA-256
        assert len(record["inputs_hash"]) == 64
        assert all(c in "0123456789abcdef" for c in record["inputs_hash"])
        # Verify guardrail_trace is boolean True (non-empty dict)
        assert record["guardrail_trace"] is True
        # Verify no raw inputs in the export record
        assert "objective" not in record
        assert "source_id" not in record

    def test_sanitise_record_null_guardrail_trace_is_false(self):
        """guardrail_trace is False when the raw value is None."""
        raw = {
            "timestamp": "2026-04-01T12:00:00+00:00",
            "tool": "discover",
            "principal": "user-abc",
            "inputs": {},
            "outputs": {"status": "complete"},
            "cost": None,
            "guardrail_trace": None,
        }
        record = _sanitise_record(raw)
        assert record["guardrail_trace"] is False

    def test_hmac_sha256_of_is_deterministic(self):
        """_hmac_sha256_of returns the same hash for the same input and key."""
        obj = {"key": "value", "num": 42}
        with patch("tools.audit_export.handler._get_hmac_key", return_value=b"stable-key"):
            h1 = _hmac_sha256_of(obj)
            h2 = _hmac_sha256_of(obj)
        assert h1 == h2
        assert len(h1) == 64

    def test_audit_export_writes_ndjson(self, s3_buckets):
        """audit_export handler writes valid NDJSON to the S3 output URI."""
        # Seed the S3 bucket that audit_export will write to.
        # The CloudWatch Logs scan will return nothing (log group absent in Substrate)
        # so we get an empty NDJSON file — we just verify the handler completes
        # and the file is created at the right URI.
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="claws-audit-export")

        import tools.audit_export.handler as _mod
        with patch.object(_mod, "_fetch_audit_records", return_value=[]):
            resp = audit_export_handler(
                {
                    "start_date": "2026-03-01",
                    "end_date": "2026-03-31",
                    "output_s3_uri": "s3://claws-audit-export/reports/march-2026.ndjson",
                },
                None,
            )

        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["status"] == "complete"
        assert body["record_count"] == 0
        assert body["output_s3_uri"] == "s3://claws-audit-export/reports/march-2026.ndjson"

        # Verify the file was written to S3
        obj = s3.get_object(Bucket="claws-audit-export", Key="reports/march-2026.ndjson")
        content = obj["Body"].read().decode("utf-8")
        assert content == ""  # empty NDJSON for zero records

    def test_audit_export_invalid_date_format(self):
        """audit_export returns 400 for invalid date format."""
        resp = audit_export_handler(
            {
                "start_date": "03/01/2026",
                "end_date": "2026-03-31",
                "output_s3_uri": "s3://bucket/key.ndjson",
            },
            None,
        )
        assert resp["statusCode"] == 400

    def test_audit_export_end_before_start(self):
        """audit_export returns 400 when end_date is before start_date."""
        resp = audit_export_handler(
            {
                "start_date": "2026-03-31",
                "end_date": "2026-03-01",
                "output_s3_uri": "s3://bucket/key.ndjson",
            },
            None,
        )
        assert resp["statusCode"] == 400
