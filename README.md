# clAWS — Controlled Excavation Tools for Agents

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![Platform](https://img.shields.io/badge/platform-Amazon%20Bedrock%20AgentCore-FF9900.svg?logo=amazon-aws&logoColor=white)](https://aws.amazon.com/bedrock/)
[![Policy](https://img.shields.io/badge/policy-Cedar-green.svg)](https://www.cedarpolicy.com/)
[![CDK](https://img.shields.io/badge/infra-CDK%20v2-FF9900.svg?logo=amazon-aws&logoColor=white)](https://docs.aws.amazon.com/cdk/v2/guide/home.html)

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

## Requirements

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (package manager)
- Node.js 18+ (for AWS CDK CLI)
- AWS CDK v2: `npm install -g aws-cdk`
- AWS account with Bedrock model access enabled for your region
- AWS credentials configured (`aws configure` or IAM role)

## Quick start

```bash
git clone https://github.com/scttfrdmn/claws.git
cd claws
uv sync --extra dev --extra cdk

cd infra/cdk
cdk deploy --all
# CDK deploys stacks in dependency order:
#   ClawsStorageStack     — S3 buckets, DynamoDB tables
#   ClawsGuardrailsStack  — Bedrock Guardrail configs
#   ClawsToolsStack       — Lambda functions, IAM roles, Athena workgroup
#   ClawsGatewayStack     — AgentCore Gateway + tool endpoint registration
#   ClawsPolicyStack      — Cedar policy deployment + gateway association
```

See [docs/getting-started.md](docs/getting-started.md) for the full walkthrough including
Bedrock model access setup, Cedar policy authoring, and running your first excavation.

## Development

```bash
# Install dev dependencies
uv sync --extra dev

# Run the test suite (155 tests, no AWS credentials required)
uv run pytest tools/ -v

# Lint and format
uv run ruff check tools/
uv run ruff format tools/

# Type check
uv run mypy tools/
```

Live integration tests against real AWS (manual, pre-release):

```bash
export CLAWS_TEST_ATHENA_DB=my_db
export CLAWS_TEST_ATHENA_TABLE=my_table
export CLAWS_TEST_ATHENA_OUTPUT=s3://my-bucket/results/
export CLAWS_TEST_RUNS_BUCKET=my-claws-runs
uv run pytest tools/tests/live/ -v -m live
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for branch conventions, commit style, and the PR process.

## Documentation

| Doc | What it covers |
|-----|---------------|
| [docs/getting-started.md](docs/getting-started.md) | Deploy and run your first excavation |
| [docs/user-guide.md](docs/user-guide.md) | Tool reference, Cedar authoring, guardrail customization |
| [docs/architecture.md](docs/architecture.md) | Component map, CDK stacks, storage systems, executors |
| [docs/safety-model.md](docs/safety-model.md) | Cedar vs Guardrails — the threat model |
| [docs/mcp-integration.md](docs/mcp-integration.md) | MCP server registry, transports, pipeline walkthrough |
| [docs/capstone-deployment.md](docs/capstone-deployment.md) | Standalone vs shared-Gateway (Capstone) deployment |
| [docs/quick-suite-integration.md](docs/quick-suite-integration.md) | Quick Suite operator surface |

## Examples

| Example | Data source | Scenario |
|---------|-------------|---------|
| [examples/genomics-excavation](examples/genomics-excavation/) | Athena | BRCA1 pathogenic variants by cohort |
| [examples/log-analysis](examples/log-analysis/) | OpenSearch | Top error patterns across microservices |
| [examples/document-mining](examples/document-mining/) | S3 Select (Parquet) | Indemnification clauses with uncapped liability |

The full API specification is in [api/openapi.yaml](api/openapi.yaml).

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

Apache 2.0 — see [LICENSE](LICENSE).
