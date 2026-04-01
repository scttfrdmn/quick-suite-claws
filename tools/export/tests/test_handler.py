"""Handler-level tests for claws.export using substrate S3."""

import hashlib
import hmac
import json
from unittest.mock import MagicMock, patch

import boto3
import pytest

import tools.export.handler as _mod
from tools.export.handler import handler
from tools.shared import store_result


@pytest.fixture(autouse=True)  # type: ignore[name-defined]
def reset_export_clients():
    _mod.EVENTS_CLIENT = None
    yield
    _mod.EVENTS_CLIENT = None


class TestExportHandler:
    def test_requires_run_id(self):
        resp = handler({"destination": {"type": "s3", "uri": "s3://bucket/out.json"}}, None)
        assert resp["statusCode"] == 400

    def test_requires_destination(self):
        resp = handler({"run_id": "run-00000000"}, None)
        assert resp["statusCode"] == 400

    def test_requires_destination_type_and_uri(self):
        resp = handler({"run_id": "run-00000000", "destination": {"type": "s3"}}, None)
        assert resp["statusCode"] == 400

    def test_s3_export_complete(self, s3_buckets):
        rows = [{"gene": "BRCA1"}, {"gene": "TP53"}]
        store_result("run-export01", rows)

        resp = handler(
            {
                "run_id": "run-export01",
                "destination": {
                    "type": "s3",
                    "uri": "s3://claws-export/results/out.json",
                },
                "include_provenance": False,
            },
            None,
        )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["status"] == "complete"
        assert "export_id" in body

        # Verify object written to destination bucket
        s3 = boto3.client("s3", region_name="us-east-1")
        obj = s3.get_object(Bucket="claws-export", Key="results/out.json")
        exported = json.loads(obj["Body"].read())
        assert len(exported) == 2

    def test_s3_export_with_provenance(self, s3_buckets):
        rows = [{"gene": "APOE"}]
        store_result("run-prov0001", rows)

        resp = handler(
            {
                "run_id": "run-prov0001",
                "destination": {
                    "type": "s3",
                    "uri": "s3://claws-export/prov/data.json",
                },
                "include_provenance": True,
            },
            None,
        )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert "provenance_uri" in body

        # Verify provenance file written alongside results
        s3 = boto3.client("s3", region_name="us-east-1")
        obj = s3.get_object(Bucket="claws-export", Key="prov/data.provenance.json")
        prov = json.loads(obj["Body"].read())
        assert prov["run_id"] == "run-prov0001"

    def test_unsupported_destination_type(self, s3_buckets):
        rows = [{"gene": "TP53"}]
        store_result("run-badtype1", rows)

        resp = handler(
            {
                "run_id": "run-badtype1",
                "destination": {"type": "ftp", "uri": "ftp://example.com/out"},
            },
            None,
        )
        assert resp["statusCode"] == 400
        assert "unsupported" in json.loads(resp["body"])["error"].lower()


class TestEventBridgeExport:
    def _mock_events(self, failed: int = 0) -> MagicMock:
        mock = MagicMock()
        mock.put_events.return_value = {"FailedEntryCount": failed, "Entries": []}
        return mock

    def test_eventbridge_export(self, s3_bucket, monkeypatch):
        """Successful EventBridge export returns status=complete."""
        rows = [{"gene": "BRCA1"}]
        store_result("run-eb000001", rows)
        monkeypatch.setattr(_mod, "EVENTS_CLIENT", self._mock_events())

        resp = handler(
            {
                "run_id": "run-eb000001",
                "destination": {
                    "type": "eventbridge",
                    "uri": "events://claws-bus/ClawsExportReady",
                },
                "include_provenance": False,
            },
            None,
        )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["status"] == "complete"
        assert "export_id" in body

    def test_eventbridge_uri_parsing(self, s3_bucket, monkeypatch):
        """event_bus and detail_type are correctly extracted from URI."""
        rows = [{"gene": "TP53"}]
        store_result("run-eb000002", rows)
        mock_events = self._mock_events()
        monkeypatch.setattr(_mod, "EVENTS_CLIENT", mock_events)

        handler(
            {
                "run_id": "run-eb000002",
                "destination": {
                    "type": "eventbridge",
                    "uri": "events://my-custom-bus/GenomicsExportReady",
                },
                "include_provenance": False,
            },
            None,
        )

        call_kwargs = mock_events.put_events.call_args[1]
        entry = call_kwargs["Entries"][0]
        assert entry["EventBusName"] == "my-custom-bus"
        assert entry["DetailType"] == "GenomicsExportReady"
        assert entry["Source"] == "claws"

    def test_eventbridge_partial_failure(self, s3_bucket, monkeypatch):
        """FailedEntryCount > 0 → handler returns 500."""
        rows = [{"gene": "APOE"}]
        store_result("run-eb000003", rows)
        monkeypatch.setattr(_mod, "EVENTS_CLIENT", self._mock_events(failed=1))

        resp = handler(
            {
                "run_id": "run-eb000003",
                "destination": {
                    "type": "eventbridge",
                    "uri": "events://claws-bus/ClawsExportReady",
                },
                "include_provenance": False,
            },
            None,
        )
        assert resp["statusCode"] == 500
        assert "failed" in json.loads(resp["body"])["error"].lower()


class TestCallbackExport:
    def _make_mock_response(self, status_code: int = 200) -> MagicMock:
        mock_resp = MagicMock()
        mock_resp.status_code = status_code
        mock_resp.raise_for_status = MagicMock()
        return mock_resp

    def test_callback_export(self, s3_bucket, monkeypatch):
        """Successful callback POST returns status=complete."""
        rows = [{"gene": "BRCA1"}]
        store_result("run-cb000001", rows)

        with patch("requests.post") as mock_post:
            mock_post.return_value = self._make_mock_response(200)
            resp = handler(
                {
                    "run_id": "run-cb000001",
                    "destination": {
                        "type": "callback",
                        "uri": "https://api.example.com/webhooks/claws",
                    },
                    "include_provenance": False,
                },
                None,
            )

        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["status"] == "complete"

    def test_callback_signature_header(self, s3_bucket, monkeypatch):
        """CLAWS_CALLBACK_SECRET causes X-Claws-Signature header to be sent."""
        rows = [{"gene": "TP53"}]
        store_result("run-cb000002", rows)
        monkeypatch.setattr(_mod, "CALLBACK_SECRET", "test-secret-key")

        with patch("requests.post") as mock_post:
            mock_post.return_value = self._make_mock_response(200)
            handler(
                {
                    "run_id": "run-cb000002",
                    "destination": {
                        "type": "callback",
                        "uri": "https://api.example.com/webhooks/claws",
                    },
                    "include_provenance": False,
                },
                None,
            )

        call_kwargs = mock_post.call_args[1]
        headers = call_kwargs["headers"]
        assert "X-Claws-Signature" in headers
        assert headers["X-Claws-Signature"].startswith("sha256=")

        # Verify HMAC correctness
        body_sent = call_kwargs["data"]
        expected_sig = hmac.new(
            b"test-secret-key", body_sent.encode(), hashlib.sha256
        ).hexdigest()
        assert headers["X-Claws-Signature"] == f"sha256={expected_sig}"

    def test_callback_http_error(self, s3_bucket, monkeypatch):
        """HTTP error from callback → handler returns 500."""
        rows = [{"gene": "APOE"}]
        store_result("run-cb000003", rows)

        from requests.exceptions import HTTPError

        with patch("requests.post") as mock_post:
            mock_resp = self._make_mock_response(500)
            mock_resp.raise_for_status.side_effect = HTTPError("500 Server Error")
            mock_post.return_value = mock_resp
            resp = handler(
                {
                    "run_id": "run-cb000003",
                    "destination": {
                        "type": "callback",
                        "uri": "https://api.example.com/webhooks/claws",
                    },
                    "include_provenance": False,
                },
                None,
            )

        assert resp["statusCode"] == 500
        assert "callback" in json.loads(resp["body"])["error"].lower()
