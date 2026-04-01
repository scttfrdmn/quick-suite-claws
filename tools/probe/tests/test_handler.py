"""Handler-level tests for claws.probe using moto Glue + DynamoDB mocks."""

import json

import boto3
import pytest
from moto import mock_aws

import tools.probe.handler as _mod
import tools.shared as _shared
from tools.probe.handler import handler


@pytest.fixture(autouse=True)
def aws_credentials(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("CLAWS_SCHEMAS_TABLE", "claws-schemas")
    monkeypatch.setenv("CLAWS_ATHENA_WORKGROUP", "primary")
    monkeypatch.setenv("CLAWS_ATHENA_OUTPUT", "s3://claws-test-output/")


@pytest.fixture(autouse=True)
def reset_clients():
    """Reset all module-level AWS client singletons."""
    _mod.GLUE_CLIENT = None
    _mod.ATHENA_CLIENT = None
    _shared._dynamodb = None
    yield
    _mod.GLUE_CLIENT = None
    _mod.ATHENA_CLIENT = None
    _shared._dynamodb = None


@pytest.fixture
def dynamo_schemas_table():
    """Create the claws-schemas DynamoDB table in moto."""
    ddb = boto3.resource("dynamodb", region_name="us-east-1")
    ddb.create_table(
        TableName="claws-schemas",
        KeySchema=[{"AttributeName": "source_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "source_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    return ddb.Table("claws-schemas")


def _create_glue_table(glue, db: str, table: str):
    glue.create_database(DatabaseInput={"Name": db})
    glue.create_table(
        DatabaseName=db,
        TableInput={
            "Name": table,
            "StorageDescriptor": {
                "Columns": [
                    {"Name": "gene", "Type": "string"},
                    {"Name": "chromosome", "Type": "string"},
                    {"Name": "position", "Type": "int"},
                ],
                "Location": f"s3://data/{db}/{table}/",
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"},
            },
            "PartitionKeys": [{"Name": "cohort", "Type": "string"}],
            "Parameters": {"recordCount": "1000000", "averageRecordSize": "200"},
        },
    )


class TestProbeHandler:
    def test_requires_source_id(self):
        resp = handler({}, None)
        assert resp["statusCode"] == 400

    @mock_aws
    def test_invalid_source_id_format(self):
        resp = handler({"source_id": "nocohere"}, None)
        assert resp["statusCode"] == 400
        assert "invalid" in json.loads(resp["body"])["error"].lower()

    @mock_aws
    def test_athena_schema_only(self, dynamo_schemas_table):
        glue = boto3.client("glue", region_name="us-east-1")
        _create_glue_table(glue, "genomics", "variants")

        resp = handler({"source_id": "athena:genomics.variants", "mode": "schema_only"}, None)
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert "schema" in body
        # Should have 3 regular columns + 1 partition key
        assert len(body["schema"]["columns"]) == 4

        # Schema should be cached in DynamoDB
        cached = dynamo_schemas_table.get_item(Key={"source_id": "athena:genomics.variants"})
        assert "Item" in cached
        assert cached["Item"]["schema"]["table"] == "variants"

    @mock_aws
    def test_athena_with_samples(self, dynamo_schemas_table):
        """Samples come back as empty list from moto Athena (no real execution), but no error."""
        glue = boto3.client("glue", region_name="us-east-1")
        _create_glue_table(glue, "genomics", "variants")

        resp = handler(
            {"source_id": "athena:genomics.variants", "mode": "schema_and_samples", "sample_rows": 5},
            None,
        )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert "schema" in body
        # samples list is present (may be empty from moto)
        assert "samples" in body
        assert isinstance(body["samples"], list)

    @mock_aws
    def test_unsupported_backend(self):
        resp = handler({"source_id": "dynamodb:my_table"}, None)
        assert resp["statusCode"] == 400
        assert "unsupported" in json.loads(resp["body"])["error"].lower()
