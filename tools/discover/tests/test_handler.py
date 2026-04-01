"""Handler-level tests for claws.discover using substrate Glue."""

import json

import boto3
import pytest

import tools.discover.handler as _mod
from tools.discover.handler import handler


@pytest.fixture(autouse=True)
def reset_clients():
    """Reset module-level Glue singleton so substrate intercepts it."""
    _mod.GLUE_CLIENT = None
    _mod.OPENSEARCH_CLIENT = None
    yield
    _mod.GLUE_CLIENT = None
    _mod.OPENSEARCH_CLIENT = None


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
