# clAWS — Controlled Excavation Tools for Agents

**Safe data excavation on Amazon Bedrock AgentCore.**

When an agent can generate arbitrary SQL against a production database, call costs are
unbounded, PII leaks are unchecked, and every query is an implicit trust decision. clAWS is
a deployable reference architecture that puts a governed, auditable tool plane between any
agent and your data stores — Athena, OpenSearch, S3, DynamoDB — so that access is
policy-gated, cost-bounded, and every action is traceable to a principal and a plan.

## What it is / what it is not

| clAWS is... | clAWS is not... |
|-------------|-----------------|
| A deployable CDK application on real AWS | A managed service or SaaS |
| A reference architecture for policy-gated data access | A query builder or BI tool |
| A tool plane an agent calls; the agent reasons elsewhere | An agent framework or LLM runtime |
| Open source under Apache 2.0 | A product with a support contract |

## Core principle: separate reasoning from execution

LLM reasoning never happens inside a tool. The `plan` tool is the only tool that accepts
free-text input. It returns a concrete, reviewable execution plan — the actual query, cost
estimate, and output schema. The `excavate` tool takes that plan verbatim. Cedar policies
gate the concrete query, not the intent.

```
Agent session (reasoning)             Tool plane (execution)
┌──────────────────────────┐          ┌───────────────────────────┐
│ "Find BRCA1 pathogenic    │          │                           │
│  variants by cohort"      │  plan ──▶│ Returns concrete SQL +    │
│                           │          │ cost estimate + schema     │
│  Reviews plan ────────────┼──────────┼▶                          │
│                           │  exec ──▶│ Runs the exact query,     │
│  Receives results         │          │ nothing else              │
└──────────────────────────┘          └───────────────────────────┘
          │                                        │
          └──────── Cedar + Guardrails ────────────┘
                    enforced at both boundaries
```

## Tool pipeline

| Tool | Purpose |
|------|---------|
| `discover` | Find data sources in approved domains (Glue, OpenSearch, S3) |
| `probe` | Inspect schema, sample rows, and cost estimates |
| `plan` | Translate a free-text objective into a concrete query (LLM + Guardrails) |
| `excavate` | Execute the exact query from the plan (Athena, OpenSearch DSL, S3 Select) |
| `refine` | Dedupe, rank, filter, and summarize results |
| `export` | Materialize to S3, EventBridge, or an HTTP callback with provenance chain |

See [docs/architecture.md](docs/architecture.md) for the full pipeline table with policy
gates, storage systems, and CDK stack details.

## Safety model

**Cedar (AgentCore Policy)** is structural and deterministic: it enforces permissions,
cost limits, source allowlists, and the plan-to-execution linkage at the Gateway boundary
before any Lambda runs. **Bedrock Guardrails** is ML-based and semantic: it detects
PII/PHI, prompt injection, denied topics, and content violations at LLM I/O and via the
`ApplyGuardrail` API on data paths. An attacker must defeat both simultaneously.

See [docs/safety-model.md](docs/safety-model.md) for attachment points and the threat model.

## Quick start

```bash
# Prerequisites: Python 3.12+, AWS CDK v2, AWS credentials configured
# See docs/getting-started.md for the full walkthrough

git clone https://github.com/scttfrdmn/claws.git
cd claws
pip install -e ".[dev,cdk]"

cd infra/cdk
cdk deploy --all
# CDK deploys stacks in dependency order:
#   ClawsStorageStack     — S3 buckets, DynamoDB tables
#   ClawsGuardrailsStack  — Bedrock Guardrail configs
#   ClawsToolsStack       — Lambda functions, IAM roles, Athena workgroup
#   ClawsGatewayStack     — AgentCore Gateway + tool endpoint registration
#   ClawsPolicyStack      — Cedar policy deployment + gateway association
```

## Documentation

| Doc | What it covers |
|-----|---------------|
| [docs/getting-started.md](docs/getting-started.md) | Deploy and run your first excavation |
| [docs/user-guide.md](docs/user-guide.md) | Tool reference, Cedar authoring, guardrail customization |
| [docs/architecture.md](docs/architecture.md) | Component map, CDK stacks, storage systems |
| [docs/safety-model.md](docs/safety-model.md) | Cedar vs Guardrails — the threat model |
| [docs/quick-suite-integration.md](docs/quick-suite-integration.md) | Optional operator surface |

## Examples

| Example | Data source | Scenario |
|---------|-------------|---------|
| [examples/genomics-excavation](examples/genomics-excavation/) | Athena | BRCA1 pathogenic variants by cohort |
| [examples/log-analysis](examples/log-analysis/) | OpenSearch | Top error patterns across microservices |
| [examples/document-mining](examples/document-mining/) | S3 Select (Parquet) | Indemnification clauses with uncapped liability |

The full API specification is in [api/openapi.yaml](api/openapi.yaml).

## License

Apache 2.0
