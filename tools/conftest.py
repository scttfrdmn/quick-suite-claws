"""Shared fixtures for all clAWS handler tests."""

import boto3
import pytest

import tools.shared as _shared


@pytest.fixture(autouse=True)
def reset_shared(monkeypatch):
    """Reset boto3 singletons and patch module-level constants before every test.

    tools/shared.py evaluates RUNS_BUCKET, PLANS_TABLE, etc. at import time, so
    monkeypatch.setenv() is too late to affect them. We patch the attributes directly.
    """
    _shared._s3 = None
    _shared._dynamodb = None
    _shared._bedrock = None
    monkeypatch.setattr(_shared, "RUNS_BUCKET", "claws-runs")
    monkeypatch.setattr(_shared, "PLANS_TABLE", "claws-plans")
    monkeypatch.setattr(_shared, "SCHEMAS_TABLE", "claws-schemas")
    monkeypatch.setattr(_shared, "GUARDRAIL_ID", "")
    yield
    _shared._s3 = None
    _shared._dynamodb = None
    _shared._bedrock = None


# ---------------------------------------------------------------------------
# Shared AWS resource fixtures (all require the `substrate` fixture for routing)
# ---------------------------------------------------------------------------

@pytest.fixture()
def s3_bucket(substrate):
    """Create the claws-runs S3 bucket."""
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="claws-runs")
    return s3


@pytest.fixture()
def s3_buckets(substrate):
    """Create both claws-runs (results) and claws-export (destination) S3 buckets."""
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="claws-runs")
    s3.create_bucket(Bucket="claws-export")
    return s3


@pytest.fixture()
def plans_table(substrate):
    """Create the claws-plans DynamoDB table."""
    ddb = boto3.resource("dynamodb", region_name="us-east-1")
    ddb.create_table(
        TableName="claws-plans",
        KeySchema=[{"AttributeName": "plan_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "plan_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    return ddb.Table("claws-plans")


@pytest.fixture()
def schemas_table(substrate):
    """Create the claws-schemas DynamoDB table."""
    ddb = boto3.resource("dynamodb", region_name="us-east-1")
    ddb.create_table(
        TableName="claws-schemas",
        KeySchema=[{"AttributeName": "source_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "source_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    return ddb.Table("claws-schemas")
