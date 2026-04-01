"""Handler-level tests for claws.export using moto S3."""

import json

import boto3
import pytest
from moto import mock_aws

import tools.shared as _shared
from tools.export.handler import handler
from tools.shared import store_result


@pytest.fixture(autouse=True)
def aws_credentials(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("CLAWS_RUNS_BUCKET", "claws-runs-test")
    monkeypatch.setenv("CLAWS_GUARDRAIL_ID", "")


@pytest.fixture(autouse=True)
def reset_clients():
    _shared._s3 = None
    yield
    _shared._s3 = None


@pytest.fixture
def s3_buckets():
    """Create source and destination buckets in moto."""
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="claws-runs-test")
    s3.create_bucket(Bucket="claws-export-test")
    return s3


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

    @mock_aws
    def test_s3_export_complete(self, s3_buckets):
        rows = [{"gene": "BRCA1"}, {"gene": "TP53"}]
        store_result("run-export01", rows)

        resp = handler(
            {
                "run_id": "run-export01",
                "destination": {"type": "s3", "uri": "s3://claws-export-test/results/out.json"},
                "include_provenance": False,
            },
            None,
        )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["status"] == "complete"
        assert "export_id" in body

        # Verify object written to destination
        s3 = boto3.client("s3", region_name="us-east-1")
        obj = s3.get_object(Bucket="claws-export-test", Key="results/out.json")
        exported = json.loads(obj["Body"].read())
        assert len(exported) == 2

    @mock_aws
    def test_s3_export_with_provenance(self, s3_buckets):
        rows = [{"gene": "APOE"}]
        store_result("run-prov0001", rows)

        resp = handler(
            {
                "run_id": "run-prov0001",
                "destination": {"type": "s3", "uri": "s3://claws-export-test/prov/data.json"},
                "include_provenance": True,
            },
            None,
        )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert "provenance_uri" in body

        # Verify provenance file written
        s3 = boto3.client("s3", region_name="us-east-1")
        obj = s3.get_object(Bucket="claws-export-test", Key="prov/data.provenance.json")
        prov = json.loads(obj["Body"].read())
        assert prov["run_id"] == "run-prov0001"

    @mock_aws
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
