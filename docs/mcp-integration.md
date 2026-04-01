# MCP Integration Guide

clAWS supports the [Model Context Protocol](https://modelcontextprotocol.io/) (MCP) as a
fourth executor type, giving agents access to 7,600+ published MCP servers — PostgreSQL,
Snowflake, MongoDB, DuckDB, REST APIs, and more — without requiring individual integrations.

## How it works

MCP servers are registered in a JSON configuration. At runtime, clAWS opens a short-lived
session to the server, calls a tool, and returns the results through the standard
excavation pipeline (plan → excavate, with guardrail scanning before results are returned).

The full tool pipeline works identically for MCP sources:

```
discover → probe → plan → excavate → refine → export
```

## Configuring MCP servers

Set the `CLAWS_MCP_SERVERS_CONFIG` environment variable to either an inline JSON string
or an S3 URI pointing to a JSON file.

### Inline JSON

```json
{
  "servers": {
    "postgres-prod": {
      "transport": "stdio",
      "command": "npx @dbhub/mcp",
      "env": {
        "CONNECTION_STRING": "postgresql://user:pass@host:5432/db"
      }
    },
    "snowflake-dw": {
      "transport": "http",
      "url": "https://mcp.snowflake.example.com"
    }
  }
}
```

### S3-hosted config

```
CLAWS_MCP_SERVERS_CONFIG=s3://my-bucket/claws/mcp-servers.json
```

### Transport types

| Transport | When to use | Required fields |
|-----------|-------------|-----------------|
| `stdio`   | Local processes, npx-based servers | `command` (space-separated), optionally `env` |
| `http`    | Remote HTTP+SSE servers | `url` |
| `sse`     | Alias for `http` | `url` |

## Using MCP sources

### discover

```json
{
  "query": "users table",
  "scope": {
    "domains": ["mcp"],
    "spaces": ["postgres-prod"]
  }
}
```

`spaces` filters by server name. Leave empty to search all registered servers.

Returns sources with IDs like `mcp://postgres-prod/public.users`.

### probe

```json
{
  "source_id": "mcp://postgres-prod/public.users",
  "mode": "schema_only"
}
```

Returns the available tools for the server and metadata for the requested resource.
`mode: "schema_and_samples"` calls the first available tool with empty arguments
as a best-effort sample — failure is suppressed and returns an empty sample list.

### plan

```json
{
  "objective": "Find all active users created in the last 30 days",
  "source_id": "mcp://postgres-prod/public.users"
}
```

The plan tool generates a JSON query object (not SQL) for MCP sources:

```json
{
  "server": "postgres-prod",
  "tool": "query",
  "arguments": {
    "sql": "SELECT * FROM public.users WHERE active = true AND created_at > NOW() - INTERVAL '30 days'"
  }
}
```

MCP plans bypass SQL validation and cost estimation — the MCP server enforces its own
access controls at the transport layer.

### excavate

Excavate takes the plan verbatim. No changes needed — the `mcp_tool` query type is
handled by the MCP executor.

## Security notes

- **Cedar policies** apply at the Gateway boundary for all requests, including MCP.
- **Bedrock Guardrails** scan MCP results for PII/PHI before returning them to the agent,
  exactly as with Athena and OpenSearch results.
- MCP server credentials (connection strings, API keys) belong in the `env` field of the
  server config, which should be stored in AWS Secrets Manager and injected at Lambda
  startup — not hardcoded in the config JSON.
- The `source_id` field in the excavate request is authoritative for server lookup.
  The `server` field inside the query JSON is informational only.

## Example: querying a PostgreSQL server via DBHub MCP

1. Install the server: `npm install -g @dbhub/mcp`
2. Configure:
   ```json
   {
     "servers": {
       "postgres-prod": {
         "transport": "stdio",
         "command": "npx @dbhub/mcp",
         "env": { "CONNECTION_STRING": "postgresql://..." }
       }
     }
   }
   ```
3. Discover: query with `domains: ["mcp"]`
4. Probe: inspect available tools and resource schema
5. Plan: generate a tool-call JSON from your objective
6. Excavate: execute and receive guardrail-scanned results
