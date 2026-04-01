"""Tests for tools.mcp.registry — Issue #22 (+2 tests)."""

import json

import pytest

import tools.mcp.registry as _reg_mod
from tools.mcp.registry import get_mcp_registry


@pytest.fixture(autouse=True)
def reset_registry(monkeypatch):
    """Reset module-level registry cache before every test."""
    monkeypatch.setattr(_reg_mod, "_MODULE_REGISTRY", None)


class TestMcpRegistry:
    def test_returns_empty_dict_when_unconfigured(self, monkeypatch):
        """No env var → returns {} without error."""
        monkeypatch.delenv("CLAWS_MCP_SERVERS_CONFIG", raising=False)
        result = get_mcp_registry()
        assert result == {}

    def test_loads_servers_from_inline_json(self, monkeypatch):
        """Inline JSON in env var is parsed and servers dict returned."""
        config = {
            "servers": {
                "postgres-prod": {
                    "transport": "stdio",
                    "command": "npx @dbhub/mcp",
                    "env": {"CONNECTION_STRING": "postgresql://localhost/db"},
                },
                "snowflake": {
                    "transport": "http",
                    "url": "https://mcp.example.com/snowflake",
                },
            }
        }
        monkeypatch.setenv("CLAWS_MCP_SERVERS_CONFIG", json.dumps(config))
        result = get_mcp_registry()
        assert "postgres-prod" in result
        assert result["postgres-prod"]["transport"] == "stdio"
        assert "snowflake" in result
        assert result["snowflake"]["transport"] == "http"

    def test_cached_after_first_call(self, monkeypatch):
        """Registry is loaded once — subsequent calls return the cached value."""
        config = {"servers": {"srv": {"transport": "stdio", "command": "x"}}}
        monkeypatch.setenv("CLAWS_MCP_SERVERS_CONFIG", json.dumps(config))

        result1 = get_mcp_registry()
        # Change env var — should not affect cached result
        monkeypatch.setenv("CLAWS_MCP_SERVERS_CONFIG", '{"servers": {}}')
        result2 = get_mcp_registry()

        assert result1 is result2
        assert "srv" in result2

    def test_returns_empty_on_invalid_json(self, monkeypatch):
        """Malformed JSON → warning printed, returns {} without raising."""
        monkeypatch.setenv("CLAWS_MCP_SERVERS_CONFIG", "not valid json {{")
        result = get_mcp_registry()
        assert result == {}

    def test_returns_empty_when_servers_key_missing(self, monkeypatch):
        """Valid JSON but no 'servers' key → returns {}."""
        monkeypatch.setenv("CLAWS_MCP_SERVERS_CONFIG", '{"config": {}}')
        result = get_mcp_registry()
        assert result == {}
