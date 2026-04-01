"""MCP client async-to-sync bridge for clAWS Lambda handlers.

Lambda handlers are synchronous. This module provides run_mcp_async(),
which bridges async MCP sessions into synchronous caller code using
asyncio.run(). Each call creates a fresh event loop — correct for Lambda
since there is no running event loop in a sync handler thread.

Supported transports:
  - stdio: spawns a local subprocess (StdioServerParameters + stdio_client)
  - http / sse: connects to an HTTP+SSE endpoint (sse_client)

Usage pattern in handlers:

    from tools.mcp.client import run_mcp_async

    async def _list_resources(session):
        result = await session.list_resources()
        return result.resources

    resources = run_mcp_async(_list_resources, server_config)
"""

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any, TypeVar

T = TypeVar("T")


def run_mcp_async(  # noqa: UP047
    coro_fn: Callable[..., Awaitable[T]],
    server_config: dict,
    **kwargs: Any,
) -> T:
    """Run an async MCP operation synchronously via a fresh event loop.

    Args:
        coro_fn: An async callable that accepts a ClientSession as its first
                 positional argument, plus any **kwargs.
        server_config: Server config dict from get_mcp_registry().
        **kwargs: Forwarded to coro_fn.

    Returns:
        Whatever coro_fn returns.

    Raises:
        Any exception raised by coro_fn propagates to the caller.
        ValueError if the transport type is not supported.
    """
    async def _run() -> T:
        async with _mcp_session(server_config) as session:
            return await coro_fn(session, **kwargs)

    return asyncio.run(_run())


@asynccontextmanager
async def _mcp_session(server_config: dict):  # type: ignore[return]
    """Async context manager yielding an initialized ClientSession.

    Dispatches on server_config["transport"]:
      - "stdio" → StdioServerParameters + stdio_client
      - "http" / "sse" → sse_client (HTTP+SSE transport)
    """
    from mcp.client.session import ClientSession  # noqa: PLC0415

    transport = server_config.get("transport", "stdio")

    if transport == "stdio":
        from mcp.client.stdio import StdioServerParameters, stdio_client  # noqa: PLC0415

        raw_command = server_config["command"]
        parts = raw_command.split()
        params = StdioServerParameters(
            command=parts[0],
            args=parts[1:] if len(parts) > 1 else [],
            env=server_config.get("env"),
        )
        async with stdio_client(params) as (read, write), ClientSession(read, write) as session:
            await session.initialize()
            yield session

    elif transport in ("http", "sse"):
        from mcp.client.sse import sse_client  # noqa: PLC0415

        url = server_config["url"]
        async with sse_client(url) as (read, write), ClientSession(read, write) as session:
            await session.initialize()
            yield session

    else:
        raise ValueError(
            f"Unsupported MCP transport: {transport!r}. Expected 'stdio', 'http', or 'sse'."
        )
