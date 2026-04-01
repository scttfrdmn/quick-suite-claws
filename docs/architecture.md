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
    └─────────┘    └─────┬─────┘   └─────┬─────┘
                         │               │
                    ┌────▼────┐    ┌──────▼──────┐
                    │ Bedrock │    │   Athena    │
                    │ (model  │    │ OpenSearch  │
                    │+ guard) │    │  S3 Select  │
                    └─────────┘    └─────────────┘
```

## Tool pipeline

| # | Tool | Input | Output | Safety gate |
|---|------|-------|--------|-------------|
| 1 | discover | topic + scope | ranked sources | Cedar: scope allowlist |
| 2 | probe | source_id | schema + samples | Cedar: source allowlist, Guardrails: PII scan |
| 3 | plan | free-text objective | concrete query + cost | Guardrails: injection + PII, Cedar: constraints |
| 4 | excavate | concrete query from plan | result rows | Cedar: plan linkage + bounds, Guardrails: PII scan |
| 5 | refine | run_id + operations | refined results | Guardrails: grounding on summaries |
| 6 | export | run_id + destination | materialized output | Cedar: destination allowlist, Guardrails: final scan |

## Storage

- **S3 (claws-runs)** — excavation results, 30-day lifecycle
- **S3 (claws-athena-results)** — Athena query output, 7-day lifecycle
- **DynamoDB (claws-plans)** — execution plans, 24-hour TTL
- **DynamoDB (claws-schemas)** — cached source schemas, 1-hour TTL

## Deployment

Python CDK stacks in `infra/cdk/`:

| Stack | Resources |
|-------|-----------|
| StorageStack | S3 buckets, DynamoDB tables |
| GuardrailsStack | Bedrock Guardrail configurations |
| ToolsStack | Lambda functions, IAM roles, Athena workgroups |
| GatewayStack | AgentCore Gateway tool registration |
| PolicyStack | Cedar policy deployment |
