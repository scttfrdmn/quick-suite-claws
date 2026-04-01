"""Handler-level tests for claws.refine using substrate S3."""

import json

import boto3

from tools.refine.handler import handler
from tools.shared import store_result


class TestRefineHandler:
    def test_requires_run_id(self):
        resp = handler({"operations": ["dedupe"]}, None)
        assert resp["statusCode"] == 400

    def test_requires_operations(self):
        resp = handler({"run_id": "run-00000000"}, None)
        assert resp["statusCode"] == 400

    def test_invalid_operation(self, s3_bucket):
        store_result("run-aaaaaaaa", [{"gene": "BRCA1"}])
        resp = handler({"run_id": "run-aaaaaaaa", "operations": ["fly_to_moon"]}, None)
        assert resp["statusCode"] == 400
        assert "invalid" in json.loads(resp["body"])["error"].lower()

    def test_dedupe(self, s3_bucket):
        rows = [{"gene": "BRCA1"}, {"gene": "BRCA1"}, {"gene": "TP53"}]
        store_result("run-dedupe01", rows)

        resp = handler({"run_id": "run-dedupe01", "operations": ["dedupe"]}, None)
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        manifest_op = body["manifest"]["operations"][0]
        assert manifest_op["rows_before"] == 3
        assert manifest_op["rows_after"] == 2

    def test_rank(self, s3_bucket):
        rows = [
            {"gene": "A", "score": "3"},
            {"gene": "B", "score": "1"},
            {"gene": "C", "score": "2"},
        ]
        store_result("run-rank0001", rows)

        resp = handler({"run_id": "run-rank0001", "operations": ["rank_by_score"]}, None)
        assert resp["statusCode"] == 200

        refined_run_id = json.loads(resp["body"])["run_id"]
        s3 = boto3.client("s3", region_name="us-east-1")
        obj = s3.get_object(Bucket="claws-runs", Key=f"{refined_run_id}/result.json")
        result_rows = json.loads(obj["Body"].read())
        scores = [float(r["score"]) for r in result_rows]
        assert scores == sorted(scores, reverse=True)

    def test_normalize(self, s3_bucket):
        rows = [{"Gene Name": "BRCA1", "Count": "42", "Ratio": "0.75"}]
        store_result("run-norm0001", rows)

        resp = handler({"run_id": "run-norm0001", "operations": ["normalize"]}, None)
        assert resp["statusCode"] == 200

        refined_run_id = json.loads(resp["body"])["run_id"]
        s3 = boto3.client("s3", region_name="us-east-1")
        obj = s3.get_object(Bucket="claws-runs", Key=f"{refined_run_id}/result.json")
        result_rows = json.loads(obj["Body"].read())

        assert len(result_rows) == 1
        row = result_rows[0]
        assert "gene_name" in row  # lowercase + underscore
        assert row["count"] == 42  # parsed to int
        assert row["ratio"] == 0.75  # parsed to float
