"""Fixtures for live AWS integration tests.

These tests run against real AWS services and are NOT part of the normal
pytest run. They are intended for manual pre-release validation only.

To run:
    pytest tools/tests/live/ -v -m live

Required environment variables:
    CLAWS_TEST_REGION          AWS region (default: us-east-1)
    CLAWS_TEST_ATHENA_DB       Glue database name for Athena tests
    CLAWS_TEST_ATHENA_TABLE    Table name within that database
    CLAWS_TEST_ATHENA_OUTPUT   s3:// URI for Athena query output
    CLAWS_TEST_RUNS_BUCKET     S3 bucket for claws-runs result storage
    CLAWS_TEST_BEDROCK_MODEL   Bedrock model ID (default: claude-sonnet-4-6)

All tests are skipped automatically if the required env vars are not set.
AWS credentials must be configured in the environment (IAM role, profile, etc.).
"""

import os

import boto3
import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _require(*var_names: str) -> dict[str, str]:
    """Return env var values or skip the test if any are missing."""
    missing = [v for v in var_names if not os.environ.get(v)]
    if missing:
        pytest.skip(f"Live test env vars not set: {', '.join(missing)}")
    return {v: os.environ[v] for v in var_names}


REGION = os.environ.get("CLAWS_TEST_REGION", "us-east-1")


# ---------------------------------------------------------------------------
# Session-scoped clients (created once, reused across all live tests)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def live_region() -> str:
    return REGION


@pytest.fixture(scope="session")
def live_s3(live_region):
    return boto3.client("s3", region_name=live_region)


@pytest.fixture(scope="session")
def live_athena(live_region):
    return boto3.client("athena", region_name=live_region)


@pytest.fixture(scope="session")
def live_bedrock(live_region):
    return boto3.client("bedrock-runtime", region_name=live_region)


# ---------------------------------------------------------------------------
# Athena config (skips if env vars not present)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def athena_config():
    return _require(
        "CLAWS_TEST_ATHENA_DB",
        "CLAWS_TEST_ATHENA_TABLE",
        "CLAWS_TEST_ATHENA_OUTPUT",
        "CLAWS_TEST_RUNS_BUCKET",
    )


# ---------------------------------------------------------------------------
# Bedrock config
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def bedrock_config():
    _require("CLAWS_TEST_RUNS_BUCKET")
    return {
        "model_id": os.environ.get(
            "CLAWS_TEST_BEDROCK_MODEL",
            "anthropic.claude-sonnet-4-20250514-v1:0",
        ),
        "runs_bucket": os.environ["CLAWS_TEST_RUNS_BUCKET"],
    }
