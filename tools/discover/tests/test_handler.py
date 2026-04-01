"""Handler-level tests for claws.discover using substrate Glue."""

import json
from unittest.mock import MagicMock

import boto3
import pytest

import tools.discover.handler as _mod
import tools.excavate.executors.opensearch as _os_mod
from tools.discover.handler import handler


@pytest.fixture(autouse=True)
def reset_clients():
    """Reset module-level singletons so substrate intercepts them."""
    _mod.GLUE_CLIENT = None
    _mod.OPENSEARCH_CLIENT = None
    _mod.S3_CLIENT = None
    yield
    _mod.GLUE_CLIENT = None
    _mod.OPENSEARCH_CLIENT = None
    _mod.S3_CLIENT = None


def _create_glue_db_and_table(glue, db_name: str, table_name: str, space: str = "research"):
    glue.create_database(
        DatabaseInput={
            "Name": db_name,
            "Parameters": {"claws:space": space},
        }
    )
    glue.create_table(
        DatabaseName=db_name,
        TableInput={
            "Name": table_name,
            "StorageDescriptor": {
                "Columns": [{"Name": "gene", "Type": "string"}],
                "Location": f"s3://data/{db_name}/{table_name}/",
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
                },
            },
            "PartitionKeys": [],
        },
    )


class TestDiscoverHandler:
    def test_requires_query(self):
        resp = handler({"scope": {"domains": ["athena"]}}, None)
        assert resp["statusCode"] == 400
        assert "query" in json.loads(resp["body"])["error"].lower()

    def test_glue_returns_sources(self, substrate):
        glue = boto3.client("glue", region_name="us-east-1")
        _create_glue_db_and_table(glue, "genomics_db", "variants_hg38")

        resp = handler(
            {
                "query": "variants",
                "scope": {"domains": ["athena"], "spaces": ["research"]},
                "limit": 10,
            },
            None,
        )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert len(body["sources"]) >= 1
        ids = [s["id"] for s in body["sources"]]
        assert "athena:genomics_db.variants_hg38" in ids

    def test_empty_result_when_no_match(self, substrate):
        glue = boto3.client("glue", region_name="us-east-1")
        _create_glue_db_and_table(glue, "genomics_db", "variants_hg38")

        resp = handler(
            {
                "query": "salary_data_xyz_nomatch",
                "scope": {"domains": ["athena"], "spaces": ["research"]},
            },
            None,
        )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["sources"] == []

    def test_limit_respected(self, substrate):
        glue = boto3.client("glue", region_name="us-east-1")
        glue.create_database(
            DatabaseInput={"Name": "big_db", "Parameters": {"claws:space": "research"}}
        )
        for i in range(20):
            glue.create_table(
                DatabaseName="big_db",
                TableInput={
                    "Name": f"variants_table_{i:02d}",
                    "StorageDescriptor": {
                        "Columns": [{"Name": "id", "Type": "string"}],
                        "Location": "s3://data/",
                        "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                        "OutputFormat": (
                            "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
                        ),
                        "SerdeInfo": {
                            "SerializationLibrary": (
                                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
                            )
                        },
                    },
                    "PartitionKeys": [],
                },
            )

        resp = handler(
            {
                "query": "variants",
                "scope": {"domains": ["athena"], "spaces": ["research"]},
                "limit": 3,
            },
            None,
        )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert len(body["sources"]) <= 3


class TestDiscoverOpenSearch:
    @pytest.fixture(autouse=True)
    def reset_os_client(self):
        _os_mod.OS_CLIENT.clear()
        yield
        _os_mod.OS_CLIENT.clear()

    def test_opensearch_discovery(self, monkeypatch):
        """_discover_opensearch returns source IDs for matching indices."""
        mock_client = MagicMock()
        mock_client.cat.indices.return_value = [
            {"index": "genes_hg38"},
            {"index": "proteins_human"},
            {"index": "logs_2024"},
        ]
        monkeypatch.setattr(_os_mod, "_os_client", lambda endpoint: mock_client)

        resp = handler(
            {
                "query": "genes",
                "scope": {
                    "domains": ["opensearch"],
                    "spaces": ["search-prod.us-east-1.es.amazonaws.com"],
                },
            },
            None,
        )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        ids = [s["id"] for s in body["sources"]]
        assert "opensearch:search-prod.us-east-1.es.amazonaws.com/genes_hg38" in ids
        # "proteins_human" and "logs_2024" don't match "genes"
        assert not any("proteins" in i or "logs" in i for i in ids)

    def test_opensearch_skips_on_error(self, monkeypatch):
        """A failing endpoint is silently skipped — no raise, empty list."""
        monkeypatch.setattr(_os_mod, "_os_client", lambda endpoint: (_ for _ in ()).throw(
            Exception("connection refused")
        ))

        resp = handler(
            {
                "query": "variants",
                "scope": {
                    "domains": ["opensearch"],
                    "spaces": ["bad-endpoint.example.com"],
                },
            },
            None,
        )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["sources"] == []


class TestDiscoverS3:
    def test_s3_discovery(self, s3_bucket):
        """Puts objects under a matching prefix; expects source IDs returned."""
        s3_bucket.put_object(Bucket="claws-runs", Key="variants/file1.csv", Body=b"data")
        s3_bucket.put_object(Bucket="claws-runs", Key="variants/file2.csv", Body=b"data")
        s3_bucket.put_object(Bucket="claws-runs", Key="logs/app.log", Body=b"data")

        resp = handler(
            {
                "query": "variants",
                "scope": {"domains": ["s3"], "spaces": ["claws-runs"]},
            },
            None,
        )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        ids = [s["id"] for s in body["sources"]]
        assert any("variants" in i for i in ids)

    def test_s3_discovery_no_match(self, s3_bucket):
        """Objects with non-matching keys return empty sources."""
        s3_bucket.put_object(Bucket="claws-runs", Key="logs/app.log", Body=b"data")

        resp = handler(
            {
                "query": "variants",
                "scope": {"domains": ["s3"], "spaces": ["claws-runs"]},
            },
            None,
        )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["sources"] == []
