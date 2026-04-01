"""MCP server registry for clAWS.

Reads server configurations from the CLAWS_MCP_SERVERS_CONFIG environment variable:
  - Inline JSON string, or
  - S3 URI (s3://bucket/key) — fetched once and cached per Lambda warm instance.

Returns an empty dict if not configured — MCP support is optional and additive.

Config format:
    {
      "servers": {
        "postgres-prod": {
          "transport": "stdio",
          "command": "npx @dbhub/mcp",
          "env": {"CONNECTION_STRING": "postgresql://..."}
        },
        "snowflake-analytics": {
          "transport": "http",
          "url": "https://mcp.snowflakecomputing.com/..."
        }
      }
    }
"""

import json
import os
from typing import Any

from tools.shared import s3_client

# Module-level cache — loaded once per Lambda warm instance (same pattern as shared.py)
_MODULE_REGISTRY: dict[str, dict] | None = None


def get_mcp_registry() -> dict[str, dict]:
    """Return the MCP server registry, loading it once per Lambda warm instance.

    Returns:
        dict[str, dict] — server_name -> server_config dict.
        Empty dict if CLAWS_MCP_SERVERS_CONFIG is not set or parsing fails.
    """
    global _MODULE_REGISTRY
    if _MODULE_REGISTRY is not None:
        return _MODULE_REGISTRY
    _MODULE_REGISTRY = _load_config()
    return _MODULE_REGISTRY


def _load_config() -> dict[str, dict]:
    """Load and parse MCP server config from env var (inline JSON or S3 URI)."""
    raw = os.environ.get("CLAWS_MCP_SERVERS_CONFIG", "").strip()
    if not raw:
        return {}

    try:
        config_text = _fetch_from_s3(raw) if raw.startswith("s3://") else raw

        config: dict[str, Any] = json.loads(config_text)
        return config.get("servers", {})

    except Exception as e:
        print(json.dumps({
            "level": "warn",
            "msg": "MCP registry load failed — MCP sources will be unavailable",
            "error": str(e),
        }))
        return {}


def _fetch_from_s3(uri: str) -> str:
    """Fetch a JSON config file from S3. Raises on any error."""
    path = uri[5:]  # strip "s3://"
    bucket, _, key = path.partition("/")
    obj = s3_client().get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8")
