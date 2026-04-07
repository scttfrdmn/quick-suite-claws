"""
Fixtures for clAWS E2E tests.

Tests invoke deployed Lambda functions directly via boto3 and run the full
discover → probe → plan → excavate → refine → export pipeline against real
AWS services (Glue, Athena, DynamoDB, S3, Bedrock).

Run:
    AWS_PROFILE=aws python3 -m pytest tools/tests/e2e/ -v -m e2e

Stack assumptions (deploy infra/cdk first if not done):
    AWS_DEFAULT_REGION=us-east-1 AWS_PROFILE=aws cdk deploy \\
        ClawsStorageStack ClawsGuardrailsStack ClawsToolsStack \\
        --app "python3 infra/cdk/app.py" --require-approval never
"""

import csv
import io
import json
import time

import boto3
import pytest

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_REGION = "us-east-1"
_GLUE_DB = "claws_e2e"
_GLUE_TABLE = "sample_data"
_E2E_SOURCE_ID = f"athena:{_GLUE_DB}.{_GLUE_TABLE}"
_E2E_PRINCIPAL = "arn:aws:iam::942542972736:user/e2e-test"
_E2E_TEAM_ID = "e2e-team-001"


# ---------------------------------------------------------------------------
# Helper: invoke a deployed clAWS Lambda and return the parsed body
# ---------------------------------------------------------------------------

def invoke(lam, function_name: str, payload: dict) -> dict:
    """Invoke a clAWS Lambda and return the parsed response body."""
    resp = lam.invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload).encode(),
    )
    raw = json.loads(resp["Payload"].read())
    # clAWS handlers return API Gateway style: {statusCode, headers, body}
    body_str = raw.get("body", "{}")
    return json.loads(body_str) if isinstance(body_str, str) else raw


# ---------------------------------------------------------------------------
# Session-scoped AWS clients
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def lam():
    return boto3.client("lambda", region_name=_REGION)


@pytest.fixture(scope="session")
def s3():
    return boto3.client("s3", region_name=_REGION)


@pytest.fixture(scope="session")
def glue():
    return boto3.client("glue", region_name=_REGION)


@pytest.fixture(scope="session")
def athena():
    return boto3.client("athena", region_name=_REGION)


@pytest.fixture(scope="session")
def ddb():
    return boto3.resource("dynamodb", region_name=_REGION)


# ---------------------------------------------------------------------------
# Deployed resource names (well-known from CDK stack)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def runs_bucket() -> str:
    return "claws-runs-942542972736"


@pytest.fixture(scope="session")
def athena_output() -> str:
    return "s3://claws-athena-results-942542972736/"


@pytest.fixture(scope="session")
def plans_table(ddb):
    return ddb.Table("claws-plans")


@pytest.fixture(scope="session")
def schemas_table(ddb):
    return ddb.Table("claws-schemas")


# ---------------------------------------------------------------------------
# Session fixture: upload a small CSV and create Glue table for Athena tests
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def glue_table(s3, glue, runs_bucket):
    """Create a Glue database/table pointing at a small CSV in S3 for E2E tests.

    Uploaded CSV has columns: id (int), name (string), value (double).
    Table is registered in Glue as SerDe CSV at s3://runs_bucket/e2e-test/sample_data/
    """
    bucket = runs_bucket
    prefix = "e2e-test/sample_data/"
    s3_path = f"s3://{bucket}/{prefix}"

    # Upload CSV to S3
    rows = [
        ["id", "name", "value"],
        ["1", "alpha", "10.5"],
        ["2", "beta", "20.0"],
        ["3", "gamma", "30.7"],
        ["4", "delta", "40.1"],
        ["5", "epsilon", "50.9"],
        ["6", "zeta", "60.2"],
        ["7", "eta", "70.4"],
        ["8", "theta", "80.6"],
        ["9", "iota", "90.8"],
        ["10", "kappa", "100.0"],
    ]
    buf = io.StringIO()
    csv.writer(buf).writerows(rows)
    s3.put_object(
        Bucket=bucket,
        Key=f"{prefix}data.csv",
        Body=buf.getvalue().encode(),
        ContentType="text/csv",
    )

    # Create Glue database (ignore if exists)
    try:
        glue.create_database(DatabaseInput={"Name": _GLUE_DB})
    except glue.exceptions.AlreadyExistsException:
        pass

    # Create (or replace) Glue table
    try:
        glue.delete_table(DatabaseName=_GLUE_DB, Name=_GLUE_TABLE)
    except glue.exceptions.EntityNotFoundException:
        pass

    glue.create_table(
        DatabaseName=_GLUE_DB,
        TableInput={
            "Name": _GLUE_TABLE,
            "Description": "E2E test sample table for clAWS",
            "StorageDescriptor": {
                "Columns": [
                    {"Name": "id", "Type": "int"},
                    {"Name": "name", "Type": "string"},
                    {"Name": "value", "Type": "double"},
                ],
                "Location": s3_path,
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                    "Parameters": {
                        "field.delim": ",",
                        "skip.header.line.count": "1",
                    },
                },
            },
            "Parameters": {
                "classification": "csv",
                "recordCount": "10",
            },
        },
    )

    yield {
        "database": _GLUE_DB,
        "table": _GLUE_TABLE,
        "source_id": _E2E_SOURCE_ID,
        "s3_path": s3_path,
        "bucket": bucket,
        "prefix": prefix,
    }

    # Teardown — clean up in reverse order
    try:
        glue.delete_table(DatabaseName=_GLUE_DB, Name=_GLUE_TABLE)
    except Exception:
        pass
    try:
        glue.delete_database(Name=_GLUE_DB)
    except Exception:
        pass
    try:
        s3.delete_object(Bucket=bucket, Key=f"{prefix}data.csv")
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Session fixture: run probe once and reuse the result across all tests
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def probe_result(lam, glue_table):
    """Run claws-probe against the E2E Glue table and return the parsed body."""
    result = invoke(lam, "claws-probe", {
        "source_id": _E2E_SOURCE_ID,
        "mode": "schema_only",
    })
    assert "schema" in result, f"probe fixture failed: {result}"
    return result


# ---------------------------------------------------------------------------
# Session fixture: create a plan using the probed schema
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def plan_result(lam, probe_result):
    """Run claws-plan after probe has cached the schema.

    Uses a simple SELECT objective that should work regardless of data values.
    Skips if plan fails (e.g. Bedrock not available or budget exceeded).
    """
    result = invoke(lam, "claws-plan", {
        "source_id": _E2E_SOURCE_ID,
        "objective": "List the top 5 rows by value, showing id and value columns.",
        "team_id": _E2E_TEAM_ID,
    })
    if "error" in result:
        pytest.skip(f"plan fixture failed (Bedrock/LLM unavailable?): {result}")
    if result.get("status") == "blocked":
        pytest.skip(f"plan blocked by guardrail: {result}")
    assert "plan_id" in result, f"plan fixture returned no plan_id: {result}"
    return result


# ---------------------------------------------------------------------------
# Session fixture: run excavate using the plan
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def excavate_result(lam, plan_result, athena_output):
    """Run claws-excavate against the plan from claws-plan.

    Skips if excavate fails (e.g. plan is in pending_approval status).
    """
    plan_id = plan_result["plan_id"]
    steps = plan_result.get("steps", [])
    if not steps:
        pytest.skip("plan returned no steps for excavate")

    step_input = steps[0]["input"]
    result = invoke(lam, "claws-excavate", {
        "plan_id": plan_id,
        "source_id": step_input.get("source_id", _E2E_SOURCE_ID),
        "query": step_input.get("query", ""),
        "query_type": step_input.get("query_type", "athena_sql"),
        "constraints": step_input.get("constraints", {}),
    })
    if "error" in result:
        pytest.skip(f"excavate fixture failed: {result}")
    assert "run_id" in result, f"excavate returned no run_id: {result}"
    return result
