"""Tests for tools/shared.py — CloudWatch metrics emission."""

from unittest.mock import MagicMock

import tools.shared as _shared


class TestEmitMetric:
    def test_skipped_when_no_namespace(self, monkeypatch):
        """emit_metric is a no-op when CLAWS_METRICS_NAMESPACE is empty."""
        monkeypatch.setattr(_shared, "METRICS_NAMESPACE", "")
        mock_cw = MagicMock()
        monkeypatch.setattr(_shared, "_cloudwatch", mock_cw)
        _shared.emit_metric("Invocations", 1.0, "Count")
        mock_cw.put_metric_data.assert_not_called()

    def test_calls_put_metric_data_when_namespace_set(self, monkeypatch):
        monkeypatch.setattr(_shared, "METRICS_NAMESPACE", "claws-test")
        mock_cw = MagicMock()
        monkeypatch.setattr(_shared, "_cloudwatch", mock_cw)
        _shared.emit_metric("Invocations", 1.0, "Count", [{"Name": "Tool", "Value": "discover"}])
        mock_cw.put_metric_data.assert_called_once()
        kwargs = mock_cw.put_metric_data.call_args[1]
        assert kwargs["Namespace"] == "claws-test"
        assert kwargs["MetricData"][0]["MetricName"] == "Invocations"
        assert kwargs["MetricData"][0]["Dimensions"] == [{"Name": "Tool", "Value": "discover"}]

    def test_swallows_cloudwatch_errors(self, monkeypatch, capsys):
        """A CloudWatch failure must not raise — metrics are best-effort."""
        monkeypatch.setattr(_shared, "METRICS_NAMESPACE", "claws-test")
        mock_cw = MagicMock()
        mock_cw.put_metric_data.side_effect = Exception("throttled")
        monkeypatch.setattr(_shared, "_cloudwatch", mock_cw)
        _shared.emit_metric("Invocations", 1.0, "Count")  # must not raise
        out = capsys.readouterr().out
        assert "emit_metric failed" in out

    def test_no_dimensions_omitted_from_payload(self, monkeypatch):
        """When dimensions=None, the Dimensions key is not sent."""
        monkeypatch.setattr(_shared, "METRICS_NAMESPACE", "claws-test")
        mock_cw = MagicMock()
        monkeypatch.setattr(_shared, "_cloudwatch", mock_cw)
        _shared.emit_metric("Invocations", 1.0, "Count")
        kwargs = mock_cw.put_metric_data.call_args[1]
        assert "Dimensions" not in kwargs["MetricData"][0]


class TestAuditLogMetrics:
    def test_emits_invocations_on_complete(self, monkeypatch):
        monkeypatch.setattr(_shared, "METRICS_NAMESPACE", "claws-test")
        emitted: list[str] = []
        monkeypatch.setattr(_shared, "emit_metric", lambda n, v, u, d=None: emitted.append(n))
        _shared.audit_log("excavate", "user1", {}, {"status": "complete", "rows_returned": 5})
        assert "Invocations" in emitted
        assert "RowsReturned" in emitted
        assert "Errors" not in emitted

    def test_emits_error_metric(self, monkeypatch):
        monkeypatch.setattr(_shared, "METRICS_NAMESPACE", "claws-test")
        emitted: list[str] = []
        monkeypatch.setattr(_shared, "emit_metric", lambda n, v, u, d=None: emitted.append(n))
        _shared.audit_log("excavate", "user1", {}, {"status": "error", "error": "boom"})
        assert "Errors" in emitted
        assert "GuardrailBlocks" not in emitted

    def test_emits_guardrail_block_metric(self, monkeypatch):
        monkeypatch.setattr(_shared, "METRICS_NAMESPACE", "claws-test")
        emitted: list[str] = []
        monkeypatch.setattr(_shared, "emit_metric", lambda n, v, u, d=None: emitted.append(n))
        _shared.audit_log("probe", "user1", {}, {"status": "blocked"})
        assert "GuardrailBlocks" in emitted
        assert "Errors" not in emitted

    def test_no_metrics_emitted_when_namespace_empty(self, monkeypatch):
        """Default test environment has METRICS_NAMESPACE="" — no CW calls."""
        monkeypatch.setattr(_shared, "METRICS_NAMESPACE", "")
        mock_cw = MagicMock()
        monkeypatch.setattr(_shared, "_cloudwatch", mock_cw)
        _shared.audit_log("discover", "user1", {}, {"status": "complete"})
        mock_cw.put_metric_data.assert_not_called()

    def test_request_id_in_audit_record(self, capsys):
        """request_id flows into the printed JSON audit record."""
        _shared.audit_log(
            "discover", "user1", {}, {"status": "complete"}, request_id="req-abc123"
        )
        import json
        record = json.loads(capsys.readouterr().out.strip())
        assert record["request_id"] == "req-abc123"
