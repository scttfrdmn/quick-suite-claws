"""Tests for the quicksight:// export destination in tools/export/handler.py."""

import json
from unittest.mock import MagicMock

import tools.export.handler as _mod


def _make_mocks(monkeypatch):
    """Wire in fake S3, QuickSight, and DynamoDB clients."""
    fake_s3 = MagicMock()
    fake_qs = MagicMock()
    fake_table = MagicMock()
    fake_dynamo = MagicMock()
    fake_dynamo.Table.return_value = fake_table

    monkeypatch.setattr(_mod, "QUICKSIGHT_ACCOUNT_ID", "123456789012")
    monkeypatch.setattr(_mod, "CLAWS_LOOKUP_TABLE", "qs-claws-lookup")
    monkeypatch.setattr(_mod, "RUNS_BUCKET", "claws-runs-test")
    monkeypatch.setattr(_mod, "_qs_client", fake_qs)

    import tools.shared as _shared
    monkeypatch.setattr(_shared, "_s3", fake_s3)
    monkeypatch.setattr(_shared, "_dynamodb", fake_dynamo)

    return fake_s3, fake_qs, fake_table


class TestExportToQuickSight:
    def test_creates_datasource_and_dataset(self, monkeypatch):
        fake_s3, fake_qs, fake_table = _make_mocks(monkeypatch)

        rows = [{"col_a": "1", "col_b": "2"}, {"col_a": "3", "col_b": "4"}]
        result = _mod._export_to_quicksight(
            "quicksight://my-dataset", rows, "run-abc123", "export-def456"
        )

        assert result["status"] == "complete"
        assert result["dataset_id"] == "claws-dset-export-def456"
        assert result["source_id"] == "claws-run-abc123"
        fake_qs.create_data_source.assert_called_once()
        fake_qs.create_data_set.assert_called_once()

    def test_writes_csv_and_manifest_to_s3(self, monkeypatch):
        fake_s3, fake_qs, fake_table = _make_mocks(monkeypatch)

        rows = [{"x": "10", "y": "20"}]
        _mod._export_to_quicksight(
            "quicksight://test-ds", rows, "run-111", "export-222"
        )

        # Two S3 puts: CSV + manifest
        assert fake_s3.put_object.call_count == 2
        csv_call = fake_s3.put_object.call_args_list[0]
        manifest_call = fake_s3.put_object.call_args_list[1]
        assert csv_call[1]["ContentType"] == "text/csv"
        assert manifest_call[1]["ContentType"] == "application/json"
        manifest = json.loads(manifest_call[1]["Body"])
        assert "fileLocations" in manifest

    def test_registers_in_lookup_table(self, monkeypatch):
        fake_s3, fake_qs, fake_table = _make_mocks(monkeypatch)

        rows = [{"val": "hello"}]
        _mod._export_to_quicksight(
            "quicksight://lookup-test", rows, "run-aaa", "export-bbb"
        )

        fake_table.put_item.assert_called_once()
        item = fake_table.put_item.call_args[1]["Item"]
        assert item["source_id"] == "claws-run-aaa"
        assert item["dataset_id"] == "claws-dset-export-bbb"

    def test_skips_lookup_table_when_not_configured(self, monkeypatch):
        fake_s3, fake_qs, fake_table = _make_mocks(monkeypatch)
        monkeypatch.setattr(_mod, "CLAWS_LOOKUP_TABLE", "")

        rows = [{"v": "1"}]
        result = _mod._export_to_quicksight(
            "quicksight://no-lookup", rows, "run-x", "export-y"
        )

        assert result["status"] == "complete"
        fake_table.put_item.assert_not_called()

    def test_returns_error_when_account_id_missing(self, monkeypatch):
        monkeypatch.setattr(_mod, "QUICKSIGHT_ACCOUNT_ID", "")

        result = _mod._export_to_quicksight(
            "quicksight://any", [{"a": "1"}], "run-1", "export-1"
        )

        assert result["status"] == "error"
        assert "QUICKSIGHT_ACCOUNT_ID" in result["error"]

    def test_returns_error_on_empty_payload(self, monkeypatch):
        monkeypatch.setattr(_mod, "QUICKSIGHT_ACCOUNT_ID", "123456789012")

        result = _mod._export_to_quicksight(
            "quicksight://empty", [], "run-2", "export-2"
        )

        assert result["status"] == "error"
        assert "No results" in result["error"]

    def test_returns_error_on_quicksight_api_failure(self, monkeypatch):
        fake_s3, fake_qs, fake_table = _make_mocks(monkeypatch)
        fake_qs.create_data_source.side_effect = Exception("QuickSight error")

        result = _mod._export_to_quicksight(
            "quicksight://fail", [{"a": "1"}], "run-3", "export-3"
        )

        assert result["status"] == "error"
        assert "Quick Sight export failed" in result["error"]

    def test_dataset_name_from_uri(self, monkeypatch):
        fake_s3, fake_qs, fake_table = _make_mocks(monkeypatch)

        rows = [{"k": "v"}]
        _result = _mod._export_to_quicksight(
            "quicksight://my-analysis-2024", rows, "run-n", "export-n"
        )

        create_ds_kwargs = fake_qs.create_data_set.call_args[1]
        assert create_ds_kwargs["Name"] == "my-analysis-2024"
