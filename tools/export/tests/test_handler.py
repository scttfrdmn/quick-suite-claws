"""Handler-level tests for claws.export using substrate S3."""

import json

import boto3

from tools.export.handler import handler
from tools.shared import store_result


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
