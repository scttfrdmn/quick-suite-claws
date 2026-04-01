"""Handler-level tests for claws.probe using substrate DynamoDB + mocked Glue columns.

Note: substrate Glue get_table does not yet return StorageDescriptor.Columns or
PartitionKeys (tracked in scttfrdmn/substrate#XXX). Until that is fixed, schema
tests mock the glue_client return value directly while still exercising the real
DynamoDB schema-caching path against substrate.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

import tools.excavate.executors.opensearch as _os_mod
import tools.probe.handler as _mod
from tools.probe.handler import handler


@pytest.fixture(autouse=True)
def reset_clients():
    """Reset module-level Glue and Athena singletons."""
    _mod.GLUE_CLIENT = None
    _mod.ATHENA_CLIENT = None
    yield
    _mod.GLUE_CLIENT = None
    _mod.ATHENA_CLIENT = None


_MOCK_TABLE = {
    "Table": {
        "Name": "variants",
        "DatabaseName": "genomics",
        "StorageDescriptor": {
            "Columns": [
                {"Name": "gene", "Type": "string"},
                {"Name": "chromosome", "Type": "string"},
                {"Name": "position", "Type": "int"},
            ],
            "Location": "s3://data/genomics/variants/",
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
            },
        },
        "PartitionKeys": [{"Name": "cohort", "Type": "string"}],
        "Parameters": {"recordCount": "1000000", "averageRecordSize": "200"},
    }
}


def _mock_glue_client() -> MagicMock:
    """Return a Glue mock that returns a complete table response."""
    mock = MagicMock()
    mock.get_table.return_value = _MOCK_TABLE
    return mock


class TestProbeHandler:
    def test_requires_source_id(self):
        resp = handler({}, None)
        assert resp["statusCode"] == 400

    def test_invalid_source_id_format(self, substrate):
        resp = handler({"source_id": "nocohere"}, None)
        assert resp["statusCode"] == 400
        assert "invalid" in json.loads(resp["body"])["error"].lower()

    def test_unsupported_backend(self, substrate):
        resp = handler({"source_id": "dynamodb:my_table"}, None)
        assert resp["statusCode"] == 400
        assert "unsupported" in json.loads(resp["body"])["error"].lower()

    def test_athena_schema_only(self, schemas_table):
        """Mock glue_client to return full column data (substrate#250 tracks the gap)."""
        with patch("tools.probe.handler.glue_client", return_value=_mock_glue_client()):
            resp = handler({"source_id": "athena:genomics.variants", "mode": "schema_only"}, None)

        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert "schema" in body
        # 3 regular columns + 1 partition key
        assert len(body["schema"]["columns"]) == 4

        # Schema should be cached in DynamoDB (real substrate DynamoDB)
        cached = schemas_table.get_item(Key={"source_id": "athena:genomics.variants"})
        assert "Item" in cached
        assert cached["Item"]["schema"]["table"] == "variants"

    def test_athena_with_samples(self, schemas_table):
        """Real Athena sampling via substrate v0.45.2+ (issue #249 resolved)."""
        with patch("tools.probe.handler.glue_client", return_value=_mock_glue_client()):
            resp = handler(
                {
                    "source_id": "athena:genomics.variants",
                    "mode": "schema_and_samples",
                    "sample_rows": 5,
                },
                None,
            )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert "schema" in body
        assert "samples" in body
        assert isinstance(body["samples"], list)


class TestProbeOpenSearch:
    @pytest.fixture(autouse=True)
    def reset_os_client(self):
        _os_mod.OS_CLIENT.clear()
        yield
        _os_mod.OS_CLIENT.clear()

    def _make_os_mock(self) -> MagicMock:
        mock = MagicMock()
        mock.indices.get_mapping.return_value = {
            "genes": {
                "mappings": {
                    "properties": {
                        "gene_id": {"type": "keyword"},
                        "symbol": {"type": "text"},
                        "score": {"type": "float"},
                    }
                }
            }
        }
        mock.indices.stats.return_value = {
            "indices": {
                "genes": {
                    "total": {
                        "docs": {"count": 5000},
                        "store": {"size_in_bytes": 2048000},
                    }
                }
            }
        }
        return mock

    def test_opensearch_schema_only(self, schemas_table, monkeypatch):
        """Schema + stats returned; schema cached in DynamoDB."""
        mock_client = self._make_os_mock()
        monkeypatch.setattr(_os_mod, "_os_client", lambda endpoint: mock_client)

        resp = handler(
            {"source_id": "opensearch:search-host.es.amazonaws.com/genes", "mode": "schema_only"},
            None,
        )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert "schema" in body
        column_names = [c["name"] for c in body["schema"]["columns"]]
        assert "gene_id" in column_names
        assert body["row_count_estimate"] == 5000

        # Schema cached in DynamoDB
        cached = schemas_table.get_item(
            Key={"source_id": "opensearch:search-host.es.amazonaws.com/genes"}
        )
        assert "Item" in cached

    def test_opensearch_with_samples(self, schemas_table, monkeypatch):
        """Samples returned when mode=schema_and_samples."""
        mock_client = self._make_os_mock()
        mock_client.search.return_value = {
            "hits": {
                "hits": [
                    {"_source": {"gene_id": "BRCA1", "symbol": "BRCA1", "score": 0.9}},
                    {"_source": {"gene_id": "TP53", "symbol": "TP53", "score": 0.8}},
                ]
            }
        }
        monkeypatch.setattr(_os_mod, "_os_client", lambda endpoint: mock_client)

        resp = handler(
            {
                "source_id": "opensearch:search-host.es.amazonaws.com/genes",
                "mode": "schema_and_samples",
                "sample_rows": 2,
            },
            None,
        )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert "samples" in body
        assert len(body["samples"]) == 2
        assert body["samples"][0]["gene_id"] == "BRCA1"

    def test_opensearch_probe_error(self, schemas_table, monkeypatch):
        """Client error sets result['error'] — no HTTP 500."""
        mock_client = MagicMock()
        mock_client.indices.get_mapping.side_effect = Exception("cluster unreachable")
        monkeypatch.setattr(_os_mod, "_os_client", lambda endpoint: mock_client)

        resp = handler(
            {"source_id": "opensearch:search-host.es.amazonaws.com/genes", "mode": "schema_only"},
            None,
        )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert "error" in body
        assert "unreachable" in body["error"]
