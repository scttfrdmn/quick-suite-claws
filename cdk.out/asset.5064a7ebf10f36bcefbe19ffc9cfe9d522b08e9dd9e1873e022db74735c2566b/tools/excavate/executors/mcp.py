"""MCP tool executor for clAWS excavate tool.

Executes MCP tool calls against registered MCP servers via the clAWS
MCP registry and async-to-sync client bridge.

Source ID format:  mcp://server-name/resource-name
Query format:      JSON string: {"server": "...", "tool": "...", "arguments": {...}}
Query type:        mcp_tool
"""

import json
from typing import Any


def _parse_source_id(source_id: str) -> tuple[str, str]:
    """Parse 'mcp://server-name/resource-name' into (server_name, resource_name).

    Raises ValueError if the source_id does not start with 'mcp://'.
    """
    if not source_id.startswith("mcp://"):
        raise ValueError(
            f"Invalid MCP source_id '{source_id}': must start with mcp://"
        )
    remainder = source_id[6:]  # strip "mcp://"
    server, _, resource = remainder.partition("/")
    if not server:
        raise ValueError(f"Missing server name in MCP source_id '{source_id}'")
    return server, resource


def _adapt_content_blocks(content: list[Any]) -> list[dict]:
    """Convert MCP content blocks into row dicts for the standard excavate response.

    Content block types handled:
      - TextContent (type="text"): JSON-parse first; fall back to {"text": ...}
      - ImageContent (type="image"): {"_type": "image", "mime_type": ..., "data": ...}
      - EmbeddedResource (type="resource") with .text: same as text branch
      - Any other type: {"_type": <type>, "data": str(block)}

    Real MCP tools (DBHub, Snowflake, etc.) typically return a single TextContent
    block containing a JSON array of row objects. The JSON-parse path handles this.
    """
    rows: list[dict] = []

    for block in content:
        block_type = getattr(block, "type", None)

        if block_type == "text":
            rows.extend(_parse_text_block(block.text))

        elif block_type == "image":
            rows.append({
                "_type": "image",
                "mime_type": getattr(block, "mimeType", ""),
                "data": getattr(block, "data", ""),
            })

        elif block_type == "resource":
            resource = getattr(block, "resource", None)
            if resource is not None and hasattr(resource, "text"):
                rows.extend(_parse_text_block(resource.text))
            else:
                rows.append({
                    "_type": "resource",
                    "uri": str(getattr(resource, "uri", "")) if resource else "",
                })

        else:
            rows.append({"_type": block_type or "unknown", "data": str(block)})

    return rows


def _parse_text_block(text: str) -> list[dict]:
    """Parse a text block into a list of row dicts."""
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return [r if isinstance(r, dict) else {"value": r} for r in parsed]
        elif isinstance(parsed, dict):
            return [parsed]
        else:
            return [{"value": parsed}]
    except (json.JSONDecodeError, ValueError):
        return [{"text": text}]


async def _call_mcp_tool(session: Any, tool_name: str, arguments: dict) -> list[dict]:
    """Async: call an MCP tool and return adapted rows."""
    result = await session.call_tool(tool_name, arguments)
    if result.isError:
        raise RuntimeError(f"MCP tool '{tool_name}' returned an error response")
    return _adapt_content_blocks(result.content)


def execute_mcp(
    source_id: str,
    query: str,
    constraints: dict,
    run_id: str,
) -> dict:
    """Execute an MCP tool call.

    Args:
        source_id: MCP source ID, e.g. "mcp://postgres-prod/public.users"
        query: JSON string {"server": "...", "tool": "...", "arguments": {...}}
        constraints: Standard excavation constraints (unused for MCP — servers
                     enforce access control at the transport layer)
        run_id: clAWS run ID (reserved for future tracing)

    Returns:
        {"status": "complete"|"error", "rows": [...], "bytes_scanned": 0, "cost": "$0.0000"}
    """
    # Parse source_id
    try:
        server_name, _ = _parse_source_id(source_id)
    except ValueError as e:
        return {"status": "error", "error": str(e)}

    # Parse query JSON
    try:
        query_obj: dict = json.loads(query)
    except (json.JSONDecodeError, ValueError) as e:
        return {"status": "error", "error": f"MCP query is not valid JSON: {e}"}

    tool_name = query_obj.get("tool", "")
    arguments = query_obj.get("arguments", {})
    if not tool_name:
        return {"status": "error", "error": "MCP query JSON missing 'tool' field"}

    # Look up server config — source_id is authoritative, not query_obj["server"]
    from tools.mcp.registry import get_mcp_registry  # noqa: PLC0415
    registry = get_mcp_registry()
    server_config = registry.get(server_name)
    if server_config is None:
        return {
            "status": "error",
            "error": f"MCP server '{server_name}' not found in registry",
        }

    # Execute via async bridge
    from tools.mcp.client import run_mcp_async  # noqa: PLC0415
    try:
        rows = run_mcp_async(
            _call_mcp_tool,
            server_config,
            tool_name=tool_name,
            arguments=arguments,
        )
    except Exception as e:
        return {"status": "error", "error": f"MCP tool execution failed: {e}"}

    return {
        "status": "complete",
        "rows": rows,
        "bytes_scanned": 0,
        "cost": "$0.0000",
    }
