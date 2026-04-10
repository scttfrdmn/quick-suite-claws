"""v1.0.0 collaboration feature tests — issues #51–#54.

Covers:
- team_id round-trip on plans (issue #51)
- claws.team_plans list tool (issue #51)
- team_id on watches and team_id_filter (issue #52)
- share_plan + excavate-as-shared-user (issue #53)
- catalog-aware discover with registry domain (issue #54)
"""

import json
import time
from unittest.mock import MagicMock, patch

import boto3
import pytest

import tools.discover.handler as _disc_mod
from tools.discover.handler import handler as discover_handler
from tools.excavate.handler import handler as excavate_handler
from tools.plan.handler import handler as plan_handler
from tools.share_plan.handler import handler as share_plan_handler
from tools.shared import cache_schema, store_plan, store_watch
from tools.team_plans.handler import handler as team_plans_handler
from tools.watches.handler import handler as watches_handler

SAMPLE_QUERY = "SELECT gene, chromosome FROM genomics.variants LIMIT 10"
SAMPLE_SCHEMA = {
    "database": "genomics",
    "table": "variants",
    "columns": [{"name": "gene", "type": "string"}, {"name": "chromosome", "type": "string"}],
    "size_bytes_estimate": 1_000_000_000,
}


def _bedrock_mock(query: str) -> MagicMock:
    body_content = json.dumps({
        "content": [
            {
                "type": "text",
                "text": json.dumps({
                    "query": query,
                    "output_schema": {"columns": ["gene", "chromosome"], "estimated_rows": 10},
                    "reasoning": "v1.0.0 test plan",
                }),
            }
        ]
    }).encode()
    mock_body = MagicMock()
    mock_body.read.return_value = body_content
    mock_client = MagicMock()
    mock_client.invoke_model.return_value = {"body": mock_body}
    return mock_client


@pytest.fixture()
def aws_resources(s3_bucket, plans_table, schemas_table):
    return s3_bucket, plans_table, schemas_table


# ---------------------------------------------------------------------------
# Issue #51 — team_id on plans + claws.team_plans
# ---------------------------------------------------------------------------

class TestTeamIdOnPlans:
    def test_plan_stores_team_id(self, aws_resources):
        """Plan created with team_id has it persisted in DynamoDB."""
        cache_schema("athena:genomics.variants", SAMPLE_SCHEMA)

        with patch("tools.plan.handler.bedrock_runtime", return_value=_bedrock_mock(SAMPLE_QUERY)):
            resp = plan_handler(
                {
                    "source_id": "athena:genomics.variants",
                    "objective": "List genes",
                    "team_id": "team-oncology",
                },
                None,
            )
        assert resp["statusCode"] == 200
        plan_id = json.loads(resp["body"])["plan_id"]

        # Load plan directly from DynamoDB to verify team_id stored
        ddb = boto3.resource("dynamodb", region_name="us-east-1")
        table = ddb.Table("claws-plans")
        item = table.get_item(Key={"plan_id": plan_id})["Item"]
        assert item.get("team_id") == "team-oncology"
        assert item.get("created_by") == "unknown"  # default principal

    def test_plan_without_team_id_has_no_team_id_key(self, aws_resources):
        """Plan created without team_id should not have team_id attribute."""
        cache_schema("athena:genomics.variants", SAMPLE_SCHEMA)

        with patch("tools.plan.handler.bedrock_runtime", return_value=_bedrock_mock(SAMPLE_QUERY)):
            resp = plan_handler(
                {"source_id": "athena:genomics.variants", "objective": "List genes"},
                None,
            )
        plan_id = json.loads(resp["body"])["plan_id"]

        ddb = boto3.resource("dynamodb", region_name="us-east-1")
        item = ddb.Table("claws-plans").get_item(Key={"plan_id": plan_id})["Item"]
        assert "team_id" not in item

    def test_team_plans_lists_team_plans(self, plans_table):
        """team_plans handler returns only plans with the given team_id."""
        store_plan("plan-t001", {
            "source_id": "athena:db.tbl",
            "query": "SELECT 1",
            "query_type": "athena_sql",
            "team_id": "team-alpha",
            "created_by": "user1",
        })
        store_plan("plan-t002", {
            "source_id": "athena:db.tbl2",
            "query": "SELECT 2",
            "query_type": "athena_sql",
            "team_id": "team-beta",
            "created_by": "user2",
        })
        store_plan("plan-t003", {
            "source_id": "athena:db.tbl3",
            "query": "SELECT 3",
            "query_type": "athena_sql",
            # no team_id
            "created_by": "user3",
        })

        resp = team_plans_handler({"team_id": "team-alpha"}, None)
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["team_id"] == "team-alpha"
        ids = [p["plan_id"] for p in body["plans"]]
        assert "plan-t001" in ids
        assert "plan-t002" not in ids
        assert "plan-t003" not in ids

    def test_team_plans_requires_team_id(self, plans_table):
        """team_plans returns 400 when team_id is missing."""
        resp = team_plans_handler({}, None)
        assert resp["statusCode"] == 400
        assert "team_id" in json.loads(resp["body"])["error"].lower()

    def test_team_plans_summary_fields(self, plans_table):
        """Each returned plan summary includes required fields."""
        store_plan("plan-sum01", {
            "source_id": "athena:db.table",
            "query": "SELECT 1",
            "query_type": "athena_sql",
            "team_id": "team-gamma",
            "created_by": "alice",
        })

        resp = team_plans_handler({"team_id": "team-gamma"}, None)
        body = json.loads(resp["body"])
        assert len(body["plans"]) == 1
        plan_summary = body["plans"][0]
        assert plan_summary["plan_id"] == "plan-sum01"
        assert plan_summary["source_id"] == "athena:db.table"
        assert plan_summary["query_type"] == "athena_sql"
        assert plan_summary["created_by"] == "alice"
        assert plan_summary["team_id"] == "team-gamma"
        assert "created_at" in plan_summary


# ---------------------------------------------------------------------------
# Issue #52 — team_id on watches + team_id_filter
# ---------------------------------------------------------------------------

class TestTeamIdOnWatches:
    def test_list_watches_team_id_filter(self, watches_table):
        """Watches with team_id are filterable by team_id_filter."""
        store_watch("watch-tm001", {
            "plan_id": "plan-x1",
            "source_id": "athena:db.t1",
            "schedule": "rate(1 day)",
            "type": "alert",
            "status": "active",
            "team_id": "team-research",
            "ttl": int(time.time()) + 86400,
        })
        store_watch("watch-tm002", {
            "plan_id": "plan-x2",
            "source_id": "athena:db.t2",
            "schedule": "rate(1 day)",
            "type": "alert",
            "status": "active",
            "team_id": "team-finance",
            "ttl": int(time.time()) + 86400,
        })
        store_watch("watch-tm003", {
            "plan_id": "plan-x3",
            "source_id": "athena:db.t3",
            "schedule": "rate(1 day)",
            "type": "alert",
            "status": "active",
            "ttl": int(time.time()) + 86400,
        })

        resp = watches_handler({"team_id_filter": "team-research"}, None)
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        ids = {w["watch_id"] for w in body["watches"]}
        assert "watch-tm001" in ids
        assert "watch-tm002" not in ids
        assert "watch-tm003" not in ids

    def test_team_id_filter_combined_with_status_filter(self, watches_table):
        """team_id_filter AND status_filter can be combined."""
        store_watch("watch-comb01", {
            "plan_id": "plan-c1",
            "source_id": "athena:db.t1",
            "schedule": "rate(1 day)",
            "type": "alert",
            "status": "active",
            "team_id": "team-combined",
            "ttl": int(time.time()) + 86400,
        })
        store_watch("watch-comb02", {
            "plan_id": "plan-c2",
            "source_id": "athena:db.t2",
            "schedule": "rate(1 day)",
            "type": "alert",
            "status": "paused",
            "team_id": "team-combined",
            "ttl": int(time.time()) + 86400,
        })

        # Filter active + team-combined → only watch-comb01
        resp = watches_handler({
            "team_id_filter": "team-combined",
            "status_filter": "active",
        }, None)
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        ids = {w["watch_id"] for w in body["watches"]}
        assert "watch-comb01" in ids
        assert "watch-comb02" not in ids


# ---------------------------------------------------------------------------
# Issue #53 — share_plan + excavate as shared user
# ---------------------------------------------------------------------------

class TestSharePlan:
    def test_owner_can_share_plan(self, aws_resources):
        """Plan owner can call share_plan to add principals to shared_with."""
        cache_schema("athena:genomics.variants", SAMPLE_SCHEMA)

        with patch("tools.plan.handler.bedrock_runtime", return_value=_bedrock_mock(SAMPLE_QUERY)):
            plan_resp = plan_handler(
                {
                    "source_id": "athena:genomics.variants",
                    "objective": "List genes for sharing test",
                    "requestContext": {"authorizer": {"principalId": "alice"}, "requestId": ""},
                },
                None,
            )
        plan_id = json.loads(plan_resp["body"])["plan_id"]

        share_resp = share_plan_handler(
            {
                "plan_id": plan_id,
                "share_with": ["bob", "carol"],
                "requestContext": {"authorizer": {"principalId": "alice"}, "requestId": ""},
            },
            None,
        )
        assert share_resp["statusCode"] == 200
        body = json.loads(share_resp["body"])
        assert body["status"] == "shared"
        assert "bob" in body["shared_with"]
        assert "carol" in body["shared_with"]

    def test_non_owner_cannot_share_plan(self, aws_resources):
        """A principal who did not create a plan cannot share it."""
        cache_schema("athena:genomics.variants", SAMPLE_SCHEMA)

        with patch("tools.plan.handler.bedrock_runtime", return_value=_bedrock_mock(SAMPLE_QUERY)):
            plan_resp = plan_handler(
                {
                    "source_id": "athena:genomics.variants",
                    "objective": "List genes",
                    "requestContext": {"authorizer": {"principalId": "alice"}, "requestId": ""},
                },
                None,
            )
        plan_id = json.loads(plan_resp["body"])["plan_id"]

        share_resp = share_plan_handler(
            {
                "plan_id": plan_id,
                "share_with": ["eve"],
                "requestContext": {"authorizer": {"principalId": "mallory"}, "requestId": ""},
            },
            None,
        )
        assert share_resp["statusCode"] == 403

    def test_shared_principal_can_excavate(self, aws_resources):
        """A principal in shared_with can excavate the plan."""
        cache_schema("athena:genomics.variants", SAMPLE_SCHEMA)

        with patch("tools.plan.handler.bedrock_runtime", return_value=_bedrock_mock(SAMPLE_QUERY)):
            plan_resp = plan_handler(
                {
                    "source_id": "athena:genomics.variants",
                    "objective": "List genes for share excavate",
                    "requestContext": {"authorizer": {"principalId": "alice"}, "requestId": ""},
                },
                None,
            )
        plan_id = json.loads(plan_resp["body"])["plan_id"]

        # Owner shares with bob
        share_plan_handler(
            {
                "plan_id": plan_id,
                "share_with": ["bob"],
                "requestContext": {"authorizer": {"principalId": "alice"}, "requestId": ""},
            },
            None,
        )

        mock_exec = {
            "status": "complete",
            "rows": [{"gene": "BRCA1", "chromosome": "17"}],
            "bytes_scanned": 512,
            "cost": "$0.0000",
        }
        with patch.dict(
            "tools.excavate.handler.EXECUTORS",
            {"athena_sql": lambda **kw: mock_exec},
        ):
            exc_resp = excavate_handler(
                {
                    "plan_id": plan_id,
                    "source_id": "athena:genomics.variants",
                    "query": SAMPLE_QUERY,
                    "query_type": "athena_sql",
                    "requestContext": {"authorizer": {"principalId": "bob"}, "requestId": ""},
                },
                None,
            )
        assert exc_resp["statusCode"] == 200

    def test_non_shared_principal_cannot_excavate(self, aws_resources):
        """A principal not in shared_with and not the owner cannot excavate."""
        cache_schema("athena:genomics.variants", SAMPLE_SCHEMA)

        with patch("tools.plan.handler.bedrock_runtime", return_value=_bedrock_mock(SAMPLE_QUERY)):
            plan_resp = plan_handler(
                {
                    "source_id": "athena:genomics.variants",
                    "objective": "List genes for non-shared test",
                    "requestContext": {"authorizer": {"principalId": "alice"}, "requestId": ""},
                },
                None,
            )
        plan_id = json.loads(plan_resp["body"])["plan_id"]

        # Share with bob only; eve should be blocked
        share_plan_handler(
            {
                "plan_id": plan_id,
                "share_with": ["bob"],
                "requestContext": {"authorizer": {"principalId": "alice"}, "requestId": ""},
            },
            None,
        )

        mock_exec = {
            "status": "complete",
            "rows": [{"gene": "BRCA1"}],
            "bytes_scanned": 512,
            "cost": "$0.0000",
        }
        with patch.dict(
            "tools.excavate.handler.EXECUTORS",
            {"athena_sql": lambda **kw: mock_exec},
        ):
            exc_resp = excavate_handler(
                {
                    "plan_id": plan_id,
                    "source_id": "athena:genomics.variants",
                    "query": SAMPLE_QUERY,
                    "query_type": "athena_sql",
                    "requestContext": {"authorizer": {"principalId": "eve"}, "requestId": ""},
                },
                None,
            )
        assert exc_resp["statusCode"] == 403

    def test_share_plan_requires_plan_id(self, plans_table):
        """share_plan returns 400 when plan_id is missing."""
        resp = share_plan_handler({"share_with": ["bob"]}, None)
        assert resp["statusCode"] == 400

    def test_share_plan_not_found(self, plans_table):
        """share_plan returns 404 for non-existent plan_id."""
        resp = share_plan_handler(
            {"plan_id": "plan-nonexistent", "share_with": ["bob"]},
            None,
        )
        assert resp["statusCode"] == 404


# ---------------------------------------------------------------------------
# Issue #54 — catalog-aware discover (registry domain)
# ---------------------------------------------------------------------------

class TestCatalogAwareDiscover:
    @pytest.fixture(autouse=True)
    def reset_discover_clients(self):
        _disc_mod.GLUE_CLIENT = None
        _disc_mod._DYNAMODB_RESOURCE = None
        _disc_mod._SSM_CLIENT = None
        _disc_mod._DATA_SOURCE_REGISTRY_TABLE = None
        yield
        _disc_mod.GLUE_CLIENT = None
        _disc_mod._DYNAMODB_RESOURCE = None
        _disc_mod._SSM_CLIENT = None
        _disc_mod._DATA_SOURCE_REGISTRY_TABLE = None

    def test_registry_domain_returns_sources(self, substrate, monkeypatch):
        """discover with domain 'registry' returns entries from the registry table."""
        # Create the source registry table in Substrate
        ddb = boto3.resource("dynamodb", region_name="us-east-1")
        ddb.create_table(
            TableName="qs-data-source-registry",
            KeySchema=[{"AttributeName": "source_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "source_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        table = ddb.Table("qs-data-source-registry")
        table.put_item(Item={
            "source_id": "roda-noaa-ghcn",
            "name": "NOAA GHCN Climate Dataset",
            "description": "Global Historical Climatology Network daily climate data",
            "source_type": "roda",
            "data_classification": "public",
            "quality_score": "0.92",
            "tags": ["climate", "noaa", "weather"],
        })
        table.put_item(Item={
            "source_id": "s3-genomics-variants",
            "name": "Genomics Variants",
            "description": "Whole genome sequencing variant calls",
            "source_type": "s3",
            "data_classification": "restricted",
            "quality_score": "0.85",
            "tags": ["genomics", "wgs"],
        })

        # Point discover at the table via env var (bypasses SSM)
        monkeypatch.setenv("DATA_SOURCE_REGISTRY_TABLE", "qs-data-source-registry")

        resp = discover_handler(
            {
                "query": "climate",
                "scope": {"domains": ["registry"], "spaces": []},
                "limit": 10,
            },
            None,
        )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert len(body["sources"]) >= 1
        ids = [s["id"] for s in body["sources"]]
        assert "roda-noaa-ghcn" in ids
        # "s3-genomics-variants" does not match "climate"
        assert "s3-genomics-variants" not in ids

    def test_registry_results_include_data_classification(self, substrate, monkeypatch):
        """Registry sources include data_classification and quality_score fields."""
        ddb = boto3.resource("dynamodb", region_name="us-east-1")
        ddb.create_table(
            TableName="qs-data-source-registry-clf",
            KeySchema=[{"AttributeName": "source_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "source_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        ddb.Table("qs-data-source-registry-clf").put_item(Item={
            "source_id": "roda-census-2020",
            "name": "US Census 2020",
            "description": "Census population and demographic data",
            "source_type": "roda",
            "data_classification": "public",
            "quality_score": "0.88",
            "tags": ["census", "demographics"],
        })

        monkeypatch.setenv("DATA_SOURCE_REGISTRY_TABLE", "qs-data-source-registry-clf")

        resp = discover_handler(
            {
                "query": "census",
                "scope": {"domains": ["registry"]},
            },
            None,
        )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert len(body["sources"]) >= 1
        src = body["sources"][0]
        assert src["data_classification"] == "public"
        assert src["quality_score"] == pytest.approx(0.88, abs=0.01)

    def test_registry_missing_config_returns_empty(self, substrate, monkeypatch):
        """When no registry table is configured, registry domain returns empty list."""
        # Ensure env var is unset and SSM call will fail
        monkeypatch.delenv("DATA_SOURCE_REGISTRY_TABLE", raising=False)

        mock_ssm = MagicMock()
        mock_ssm.get_parameter.side_effect = Exception("ParameterNotFound")
        monkeypatch.setattr(_disc_mod, "_SSM_CLIENT", mock_ssm)

        resp = discover_handler(
            {
                "query": "anything",
                "scope": {"domains": ["registry"]},
            },
            None,
        )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["sources"] == []
