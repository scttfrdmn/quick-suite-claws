# Architecture

## One-sentence summary

clAWS is a secure excavation tool plane on Amazon Bedrock AgentCore,
with Cedar policies for structural enforcement, Bedrock Guardrails for
content safety, and Amazon Quick Suite as an optional operator surface.

## Core design decision: separate reasoning from execution

LLM reasoning never happens inside a tool. The `plan` tool is the only
tool that accepts free-text input; it returns a concrete, reviewable
execution plan. The `excavate` tool takes that plan verbatim and executes
it. Cedar policies gate the concrete query, not the intent.

## Component map

```
┌─────────────────────────────────────────────────────────┐
│                    Agent Session                         │
│              (AgentCore Runtime)                         │
│                                                          │
│  Objective → discover → probe → plan → excavate → ...   │
└────────────────────────┬────────────────────────────────┘
                         │
                    ┌────▼────┐
                    │ Gateway │  ← Cedar policies enforced here
                    └────┬────┘
                         │
         ┌───────────────┼───────────────┐
         │               │               │
    ┌────▼────┐    ┌─────▼─────┐   ┌─────▼─────┐
    │ Lambda  │    │  Lambda   │   │  Lambda   │
    │discover │    │   plan    │   │ excavate  │
    │  probe  │    │  refine   │   │  export   │
    │  watch  │    └─────┬─────┘   └─────┬─────┘
    │ watches │          │               │
    └─────────┘    ┌─────▼────┐    ┌─────▼───────┐
                   │ Bedrock  │    │   Athena    │
                   │ (model + │    │ OpenSearch  │
                   │  guard)  │    │  S3 Select  │
                   └──────────┘    │     MCP     │
                                   └─────────────┘

EventBridge Scheduler ──→ Watch Runner Lambda
  (one schedule per watch)   (executes locked plan,
                              evaluates condition,
                              fires export if triggered)
```

## Tool pipeline

| # | Tool | Input | Output | Safety gate |
|---|------|-------|--------|-------------|
| 1 | `discover` | topic + scope | ranked sources | Cedar: scope allowlist |
| 2 | `probe` | source_id | schema + samples | Cedar: source allowlist; Guardrails: PII scan |
| 3 | `plan` | free-text objective | concrete query + cost | Guardrails: injection + PII; Cedar: constraints |
| 4 | `excavate` | concrete query from plan | result rows | Cedar: plan linkage + bounds; Guardrails: PII scan |
| 5 | `refine` | run_id + operations | refined results | Guardrails: grounding on summaries |
| 6 | `export` | run_id + destination | materialized output | Cedar: destination allowlist; Guardrails: final scan |
| 7 | `watch` *(v0.7)* | plan_id + schedule + condition | watch_id | Cedar: plan ownership; plan must exist and be approved |
| 8 | `watches` *(v0.7)* | optional filters | watch list + status | Cedar: principal scoping |

**Persistence note:** `watch` locks a plan at creation. The scheduled runner executes the stored
query verbatim — no LLM is invoked at run time. Safer than on-demand: the query is immutable
for the lifetime of the watch.

## Storage

| Store | Purpose | Retention |
|-------|---------|-----------|
| S3 `claws-runs` | Excavation results | 30-day lifecycle |
| S3 `claws-athena-results` | Athena query output | 7-day lifecycle |
| DynamoDB `claws-plans` | Execution plans | 24-hour TTL |
| DynamoDB `claws-schemas` | Cached source schemas | 1-hour TTL |
| DynamoDB `claws-watches` *(v0.7)* | Watch specs + run state | Configurable TTL (default 90 days) |

## Executors

| Executor | Source type | Notes |
|----------|------------|-------|
| `athena.py` | `athena:db.table` | Polls for completion; bytes-scanned cost calc |
| `opensearch.py` | `opensearch:index` | Aggregation flattening; scroll support |
| `s3_select.py` | `s3:bucket/prefix` | Parquet/CSV/JSON via S3 Select |
| `mcp.py` | `mcp://server/resource` | Async bridge via `asyncio.run()`; zero cost |

## Deployment

Python CDK stacks in `infra/cdk/`, deployed in dependency order:

| Stack | Resources |
|-------|-----------|
| `StorageStack` | S3 buckets, DynamoDB tables (incl. `claws-watches` in v0.7) |
| `GuardrailsStack` | Bedrock Guardrail configurations |
| `ToolsStack` | 6 tool Lambda functions, IAM roles, Athena workgroup |
| `SchedulerStack` *(v0.7)* | Watch runner Lambda, EventBridge Scheduler group |
| `GatewayStack` | AgentCore Gateway + tool endpoint registration |
| `PolicyStack` | Cedar policy deployment + gateway association |

**Capstone mode:** pass `-c CLAWS_GATEWAY_ID=agr-...` to reuse an existing shared
AgentCore Gateway instead of creating a new one. See `docs/capstone-deployment.md`.
