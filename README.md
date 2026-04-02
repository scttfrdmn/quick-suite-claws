# clAWS — Controlled Excavation Tools for Agents

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![Platform](https://img.shields.io/badge/platform-Amazon%20Bedrock%20AgentCore-FF9900.svg?logo=amazon-aws&logoColor=white)](https://aws.amazon.com/bedrock/)
[![Policy](https://img.shields.io/badge/policy-Cedar-green.svg)](https://www.cedarpolicy.com/)
[![CDK](https://img.shields.io/badge/infra-CDK%20v2-FF9900.svg?logo=amazon-aws&logoColor=white)](https://docs.aws.amazon.com/cdk/v2/guide/home.html)

**Safe, auditable, policy-gated data queries for AI agents — without opening your databases to arbitrary SQL.**

When an AI agent can generate and execute arbitrary SQL against a production database,
a few things go wrong very quickly. Query costs are unbounded — a careless `SELECT *`
against a 2-billion-row Athena table runs up hundreds of dollars in seconds. PII leaks
are unchecked — a query that joins student records with grades and addresses might surface
a combination of fields that no individual query was supposed to expose. And there's no
audit trail — if a compliance officer asks "what did the AI query, and why was it
allowed?", the answer is a Lambda execution log, not a governed record.

clAWS is a deployable tool plane that goes between any agent and your data stores —
Athena tables, OpenSearch indices, S3 files — and enforces structured access policies,
cost limits, and content safety on every query. The agent proposes what it wants in plain
language. clAWS translates that into a reviewable, cost-estimated query plan. Cedar
policies gate whether that plan is permitted. Bedrock Guardrails scan what comes back.
Only then does anything reach the agent.

Universities use this when the question involves restricted data: financial aid records,
student PII, research databases with IRB constraints, or sponsored program financials.
The answer the analyst or agent gets is correct and governed — and there's a complete
record of every step for compliance review.

## What clAWS Is and Isn't

| clAWS is... | clAWS is not... |
|-------------|-----------------|
| A deployable CDK application on real AWS | A managed service or hosted product |
| A policy-gated tool plane an agent calls | An agent framework — reasoning happens outside |
| Safe access to Athena, OpenSearch, and S3 | A query builder, BI tool, or SQL IDE |
| A reference architecture for governed data access | A product with a support contract |
| Open source under Apache 2.0 | Specific to Quick Suite — any AgentCore agent can use it |

## The Core Safety Principle

**LLM reasoning never happens inside a tool.** The `plan` tool is the only tool that
accepts free-text input. It returns a concrete, reviewable execution plan: the actual
SQL, a cost estimate, and the output schema. The `excavate` tool takes that plan verbatim
and runs it. Cedar policies gate the concrete query, not the intent.

This means the query that runs is always the query that was approved. A separate SQL
string cannot be substituted after Cedar validates the plan — `excavate` verifies the
submitted query matches the stored plan byte for byte before executing anything.

```
Agent (reasons here)                  clAWS tool plane (executes here)
┌──────────────────────────┐          ┌────────────────────────────────┐
│ "Find BRCA1 pathogenic    │          │                                │
│  variants by cohort"      │──plan──▶ │ Returns SQL + cost + schema    │
│                           │          │ Cedar validates: permitted?    │
│  Reviews plan ────────────┼──────────│▶                               │
│                           │──exec──▶ │ Runs the exact approved query  │
│  Receives results ◀───────│──────────│ Guardrails scan the output     │
└──────────────────────────┘          └────────────────────────────────┘
        │                                          │
        └──────── Cedar + Bedrock Guardrails ───────┘
                  enforced at both boundaries
```

## The Tool Pipeline

| Tool | What it does |
|------|-------------|
| `discover` | Find data sources in approved domains (Glue catalog, OpenSearch, S3) |
| `probe` | Inspect schema, sample rows, and cost estimates; ApplyGuardrail scans samples for PHI |
| `plan` | Translate a free-text objective into a concrete query — the only tool with free-text input |
| `excavate` | Execute the exact query from the plan; results scanned by ApplyGuardrail |
| `refine` | Deduplicate, rank, and summarize results with a grounding guardrail |
| `export` | Write results to S3, EventBridge, or Quick Sight with a provenance chain |

A typical session through the pipeline:

*"Which financial aid records for the 2024 cohort have missing FAFSA completion dates,
broken down by demographic category?"*

1. `discover` — finds the financial aid Athena table in the `institutional` domain
2. `probe` — previews the schema; GuardRail scans for SSN exposure in samples
3. `plan` — translates the question into SQL; Cedar confirms the financial aid team can run this query
4. `excavate` — runs the query; Guardrails scan the row-level output
5. `refine` — produces a clean, deduplicated summary by category
6. `export` — writes to S3 with a `.provenance.json` sidecar recording the full chain

The compliance office gets the export plus a documented record of what was queried, by
whom, under which Cedar policy, and when — automatically, without anyone building a
separate audit workflow.

## Two Independent Safety Layers

**Cedar (structural, deterministic)** — evaluated at the AgentCore Gateway boundary
before any Lambda runs. Cedar policies express rules like:
- "The IR team can query enrollment tables but not the SSN column"
- "The financial aid office can aggregate, but not export row-level student records"
- "Any query must have `read_only: true` and `max_cost_dollars` ≤ 10"

Cedar either allows or denies — no probabilistic judgment. If a policy denies, the
pipeline stops before a single Athena byte is scanned.

**Bedrock Guardrails (semantic, ML-based)** — applied at LLM I/O (the `plan` tool) and
via the `ApplyGuardrail` API directly on data (probe samples, excavation results, export
payloads). Catches things Cedar can't: a query result that technically passes the column
allowlist but whose combination of fields reconstructs PII, or a result summary that
contains injection-style content from the data itself.

An attacker who wanted to bypass both layers would need to simultaneously fool a
deterministic policy engine and a content safety model. In practice, the two layers
catch entirely different threat classes.

## Requirements

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (package manager)
- Node.js 18+ and AWS CDK v2: `npm install -g aws-cdk`
- AWS account with Bedrock model access enabled for your region
- AWS credentials configured (`aws configure` or an IAM role)

## Quick Start

```bash
git clone https://github.com/scttfrdmn/claws.git
cd claws
uv sync --extra dev --extra cdk

cd infra/cdk
cdk deploy --all
```

CDK deploys five stacks in dependency order:

| Stack | What it creates |
|-------|----------------|
| `ClawsStorageStack` | S3 buckets, DynamoDB tables (plans, schemas, lookup) |
| `ClawsGuardrailsStack` | Bedrock Guardrail with content filters, PII detection, injection blocking |
| `ClawsToolsStack` | Six Lambda functions, shared IAM role, Athena workgroup |
| `ClawsGatewayStack` | AgentCore Gateway with one Lambda target per tool |
| `ClawsPolicyStack` | Cedar policy deployment and gateway association |

Deployment takes 5–10 minutes. Save the Gateway ID from the outputs — you'll need it
if deploying other Quick Suite extensions that share this gateway.

## Capstone Deployment (Shared Gateway)

When deploying alongside the other Quick Suite extensions, clAWS can attach to an
existing shared AgentCore Gateway rather than creating its own:

```bash
cdk deploy --all -c CLAWS_GATEWAY_ID=agr-abc123
```

Get the Gateway ID from the Router stack's CloudFormation outputs:

```bash
aws cloudformation describe-stacks \
  --stack-name QuickSuiteRouterStack \
  --query 'Stacks[0].Outputs[?OutputKey==`GatewayId`].OutputValue' \
  --output text
```

## Adding Your Data Sources

After deploying, tag your Glue tables so `discover` can find them:

```bash
aws glue tag-resource \
  --resource-arn arn:aws:glue:us-east-1:123456789012:table/mydb/mytable \
  --tags-to-add "claws:space=your-space-name"
```

Then add the space to the principal's `approved_spaces` list in your Cedar policy:

```cedar
permit(
  principal in Group::"institutional-research",
  action == Action::"excavate",
  resource
) when {
  context.source.space in ["enrollment", "your-space-name"]
};
```

See [docs/user-guide.md](docs/user-guide.md) for the full Cedar policy authoring guide.

## Development and Testing

```bash
# Run the full test suite (155 tests, no AWS credentials required)
uv run pytest tools/ -v

# Lint and format
uv run ruff check tools/
uv run ruff format tools/

# Type check
uv run mypy tools/
```

For live integration tests against real AWS resources (manual, pre-release):

```bash
export CLAWS_TEST_ATHENA_DB=your_db
export CLAWS_TEST_ATHENA_TABLE=your_table
export CLAWS_TEST_ATHENA_OUTPUT=s3://your-bucket/results/
export CLAWS_TEST_RUNS_BUCKET=your-claws-runs-bucket
uv run pytest tools/tests/live/ -v -m live
```

## Cost

clAWS itself has minimal infrastructure cost — under $3/month at idle. The meaningful
cost is Athena query charges ($5 per TB scanned), which Cedar cost limits and partition
pruning keep in check. A well-written Cedar policy with `max_cost_dollars: 1.00` ensures
no single query can spend more than a dollar regardless of how large the table is.

## Documentation

| Doc | What it covers |
|-----|---------------|
| [docs/getting-started.md](docs/getting-started.md) | Deploy and run your first excavation, step by step |
| [docs/user-guide.md](docs/user-guide.md) | Tool reference, Cedar policy authoring, guardrail customization |
| [docs/architecture.md](docs/architecture.md) | CDK stacks, storage layout, executor details |
| [docs/safety-model.md](docs/safety-model.md) | Cedar vs Guardrails — the threat model and attachment points |
| [docs/mcp-integration.md](docs/mcp-integration.md) | MCP server registry, transport options, pipeline walkthrough |
| [docs/capstone-deployment.md](docs/capstone-deployment.md) | Standalone vs shared-Gateway (Capstone) deployment |
| [docs/quick-suite-integration.md](docs/quick-suite-integration.md) | Quick Suite operator surface: Flows, Automate, dashboards |

## Examples

| Example | Data source | Scenario |
|---------|-------------|---------|
| [genomics-excavation](examples/genomics-excavation/) | Athena | BRCA1 pathogenic variants by cohort |
| [log-analysis](examples/log-analysis/) | OpenSearch | Top error patterns across microservices |
| [document-mining](examples/document-mining/) | S3 Select / Parquet | Indemnification clauses with uncapped liability |

## Contributing

Contributions welcome — see [CONTRIBUTING.md](CONTRIBUTING.md) for branch conventions,
commit style, and the PR process.

## License

Apache-2.0 — Copyright 2026 Scott Friedman
