"""Unit tests for execute_mcp() and helpers — Issue #25 (+8 tests)."""

import json
from unittest.mock import MagicMock, patch

import pytest

import tools.mcp.registry as _reg_mod
from tools.excavate.executors.mcp import (
    _adapt_content_blocks,
    _parse_source_id,
    execute_mcp,
)


@pytest.fixture(autouse=True)
def reset_registry(monkeypatch):
    """Reset module-level registry cache before every test."""
    monkeypatch.setattr(_reg_mod, "_MODULE_REGISTRY", None)


def _text_block(text: str) -> MagicMock:
    b = MagicMock()
    b.type = "text"
    b.text = text
    return b


def _image_block(mime: str = "image/png", data: str = "abc=") -> MagicMock:
    b = MagicMock()
    b.type = "image"
    b.mimeType = mime
    b.data = data
    return b


def _seed_registry(monkeypatch, transport: str = "stdio") -> None:
    config = {
        "postgres-prod": {
            "transport": transport,
            "command": "npx @dbhub/mcp" if transport == "stdio" else None,
            "url": "https://mcp.example.com" if transport in ("http", "sse") else None,
        }
    }
    monkeypatch.setattr(_reg_mod, "_MODULE_REGISTRY", config)


# --- _parse_source_id ---

class TestParseSourceId:
    def test_valid_with_resource(self):
        server, resource = _parse_source_id("mcp://postgres-prod/public.users")
        assert server == "postgres-prod"
        assert resource == "public.users"

    def test_valid_no_resource(self):
        server, resource = _parse_source_id("mcp://postgres-prod/")
        assert server == "postgres-prod"
        assert resource == ""

    def test_invalid_prefix_raises(self):
        with pytest.raises(ValueError, match="must start with mcp://"):
            _parse_source_id("athena:db.table")


# --- _adapt_content_blocks ---

class TestAdaptContentBlocks:
    def test_text_json_list(self):
        payload = json.dumps([{"gene": "BRCA1"}, {"gene": "TP53"}])
        rows = _adapt_content_blocks([_text_block(payload)])
        assert len(rows) == 2
        assert rows[0]["gene"] == "BRCA1"

    def test_text_json_dict(self):
        rows = _adapt_content_blocks([_text_block(json.dumps({"count": 42}))])
        assert rows == [{"count": 42}]

    def test_text_plain_string(self):
        rows = _adapt_content_blocks([_text_block("hello world")])
        assert rows == [{"text": "hello world"}]

    def test_image_block(self):
        rows = _adapt_content_blocks([_image_block("image/png", "base64==")])
        assert rows[0]["_type"] == "image"
        assert rows[0]["mime_type"] == "image/png"

    def test_empty_content(self):
        assert _adapt_content_blocks([]) == []


# --- execute_mcp ---

class TestExecuteMcp:
    def test_stdio_tool_success(self, monkeypatch):
        """Successful stdio tool call returns complete with rows."""
        _seed_registry(monkeypatch, "stdio")
        rows_data = [{"id": 1, "name": "Alice"}]

        with patch("tools.mcp.client.run_mcp_async") as mock_run:
            mock_run.return_value = rows_data
            result = execute_mcp(
                source_id="mcp://postgres-prod/public.users",
                query=json.dumps({"server": "postgres-prod", "tool": "query",
                                  "arguments": {"sql": "SELECT * FROM users"}}),
                constraints={},
                run_id="run-mcp0001",
            )

        assert result["status"] == "complete"
        assert result["rows"] == rows_data
        assert result["cost"] == "$0.0000"
        assert result["bytes_scanned"] == 0

    def test_http_transport_success(self, monkeypatch):
        """HTTP transport resolves same way — server_config differs only."""
        _seed_registry(monkeypatch, "http")

        with patch("tools.mcp.client.run_mcp_async") as mock_run:
            mock_run.return_value = [{"result": "ok"}]
            result = execute_mcp(
                source_id="mcp://postgres-prod/schema",
                query=json.dumps({"server": "postgres-prod", "tool": "list_tables",
                                  "arguments": {}}),
                constraints={},
                run_id="run-mcp0002",
            )

        assert result["status"] == "complete"

    def test_missing_server_in_registry(self, monkeypatch):
        """Server not in registry → status=error."""
        monkeypatch.setattr(_reg_mod, "_MODULE_REGISTRY", {})
        result = execute_mcp(
            source_id="mcp://nonexistent/resource",
            query=json.dumps({"server": "nonexistent", "tool": "query", "arguments": {}}),
            constraints={},
            run_id="run-mcp0003",
        )
        assert result["status"] == "error"
        assert "not found in registry" in result["error"]

    def test_malformed_json_query(self, monkeypatch):
        """Non-JSON query string → status=error without raising."""
        _seed_registry(monkeypatch)
        result = execute_mcp(
            source_id="mcp://postgres-prod/table",
            query="not valid json {{{",
            constraints={},
            run_id="run-mcp0004",
        )
        assert result["status"] == "error"
        assert "not valid JSON" in result["error"]

    def test_invalid_source_id(self):
        """source_id without mcp:// prefix → status=error."""
        result = execute_mcp(
            source_id="athena:db.table",
            query=json.dumps({"server": "x", "tool": "y", "arguments": {}}),
            constraints={},
            run_id="run-mcp0005",
        )
        assert result["status"] == "error"
        assert "mcp://" in result["error"]

    def test_tool_execution_exception(self, monkeypatch):
        """Exception from run_mcp_async propagates as status=error."""
        _seed_registry(monkeypatch)

        with patch("tools.mcp.client.run_mcp_async") as mock_run:
            mock_run.side_effect = Exception("Connection refused")
            result = execute_mcp(
                source_id="mcp://postgres-prod/table",
                query=json.dumps({"server": "postgres-prod", "tool": "query",
                                  "arguments": {"sql": "SELECT 1"}}),
                constraints={},
                run_id="run-mcp0006",
            )

        assert result["status"] == "error"
        assert "Connection refused" in result["error"]

    def test_response_adaptation(self, monkeypatch):
        """Multiple rows from run_mcp_async are all included."""
        _seed_registry(monkeypatch)
        adapted = [{"row": 1}, {"row": 2}, {"row": 3}]

        with patch("tools.mcp.client.run_mcp_async") as mock_run:
            mock_run.return_value = adapted
            result = execute_mcp(
                source_id="mcp://postgres-prod/t",
                query=json.dumps({"server": "postgres-prod", "tool": "query",
                                  "arguments": {}}),
                constraints={},
                run_id="run-mcp0007",
            )

        assert len(result["rows"]) == 3

    def test_registry_name_mismatch(self, monkeypatch):
        """Registry loaded but server name doesn't match source_id → status=error."""
        monkeypatch.setattr(_reg_mod, "_MODULE_REGISTRY", {"other-server": {}})
        result = execute_mcp(
            source_id="mcp://postgres-prod/table",
            query=json.dumps({"server": "postgres-prod", "tool": "q", "arguments": {}}),
            constraints={},
            run_id="run-mcp0008",
        )
        assert result["status"] == "error"
        assert "postgres-prod" in result["error"]
