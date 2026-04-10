# clAWS — Controlled Excavation Tools for Agents

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![Platform](https://img.shields.io/badge/platform-Amazon%20Bedrock%20AgentCore-FF9900.svg?logo=amazon-aws&logoColor=white)](https://aws.amazon.com/bedrock/)
[![Policy](https://img.shields.io/badge/policy-Cedar-green.svg)](https://www.cedarpolicy.com/)
[![CDK](https://img.shields.io/badge/infra-CDK%20v2-FF9900.svg?logo=amazon-aws&logoColor=white)](https://docs.aws.amazon.com/cdk/v2/guide/home.html)

**Safe, auditable, policy-gated data queries for AI agents — with proactive intelligence and institutional memory.**

When an AI agent can generate and execute arbitrary SQL against a production database,
a few things go wrong very quickly. Query costs are unbounded — a careless `SELECT *`
against a 2-billion-row Athena table runs up hundreds of dollars in seconds. PII leaks
are unchecked — a query that joins student records with grades and addresses might surface
a combination of fields that no individual query was supposed to expose. And there's no
audit trail — if a compliance officer asks "what did the AI query, and why was it
allowed?", the answer is a Lambda execution log, not a governed record.

clAWS is a deployable tool plane that goes between any agent and your data stores —
Athena tables, OpenSearch indices, S3 files, PostgreSQL databases, Redshift warehouses —
and enforces structured access policies, cost limits, and content safety on every query.
The agent proposes what it wants in plain language. clAWS translates that into a
reviewable, cost-estimated query plan. Cedar policies gate whether that plan is permitted.
Bedrock Guardrails scan what comes back. Only then does anything reach the agent.

Beyond reactive queries, clAWS provides **proactive intelligence**: scheduled watches that
monitor for new grants, relevant publications, compliance gaps, and accreditation evidence
— surfacing findings before anyone asks. An **institutional memory** layer persists
findings across sessions and makes them queryable as QuickSight datasets.

## What clAWS Is and Isn't

| clAWS is... | clAWS is not... |
|-------------|-----------------|
| A deployable CDK application on real AWS | A managed service or hosted product |
| A policy-gated tool plane an agent calls | An agent framework — reasoning happens outside |
| Safe access to Athena, OpenSearch, S3, PostgreSQL, and Redshift | A query builder, BI tool, or SQL IDE |
| Proactive intelligence with scheduled watches and memory | A notification system or alerting platform |
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

**Tool Lambdas (AgentCore targets):**

| Tool | What it does |
|------|-------------|
| `discover` | Find data sources in approved domains (Glue catalog, OpenSearch, S3, Data source registry) |
| `probe` | Inspect schema, sample rows, and cost estimates; ApplyGuardrail scans samples for PHI |
| `plan` | Translate a free-text objective into a concrete query — the only tool with free-text input; supports `is_template=True` for reusable `{{variable}}` templates |
| `excavate` | Execute the exact query from the plan; Athena, OpenSearch, S3 Select, DynamoDB PartiQL, PostgreSQL, and Redshift backends; column-level access control |
| `refine` | Deduplicate, rank, and summarize results with a grounding guardrail |
| `export` | Write results to S3/EventBridge with provenance chain; destination allowlist; HTTPS enforcement |
| `team_plans` | List all plans for a team_id (read-only summaries) |
| `share_plan` | Grant or revoke another principal's access to a plan |
| `instantiate_plan` | Create a concrete plan from a template by substituting `{{variable}}` placeholders |
| `watch` | Create, update, or delete a scheduled watch on a locked plan |
| `watches` | List active watches and their last-run status |
| `remember` | Write structured finding to institutional memory (NDJSON append with ETag conditional write) |
| `recall` | Query institutional memory with structural filters (date, severity, tags, keyword) |

**Internal Lambdas (not AgentCore targets):**

| Lambda | What it does |
|--------|-------------|
| `approve_plan` | IRB reviewer approves a `pending_approval` plan; validates approver allowlist; blocks self-approval |
| `audit_export` | Exports CloudWatch audit records to NDJSON in S3 with HMAC-SHA-256-hashed I/O fields |
| `claws-watch-runner` | Scheduled Lambda invoked by EventBridge Scheduler; executes locked plans; supports five watch types |

A typical session through the pipeline:

*"Which financial aid records for the 2024 cohort have missing FAFSA completion dates,
broken down by demographic category?"*

1. `discover` — finds the financial aid Athena table in the `institutional` domain
2. `probe` — previews the schema; Guardrail scans for SSN exposure in samples
3. `plan` — translates the question into SQL; Cedar confirms the financial aid team can run this query and access these columns
4. `excavate` — runs the query; Guardrails scan the row-level output
5. `refine` — produces a clean, deduplicated summary by category
6. `export` — writes to S3 with a `.provenance.json` sidecar recording the full chain

## Two Independent Safety Layers

**Cedar (structural, deterministic)** — evaluated at the AgentCore Gateway boundary
before any Lambda runs. Cedar policies express rules like:
- "The IR team can query enrollment tables but not the SSN column"
- "The financial aid office can aggregate, but not export row-level student records"
- "Any query must have `read_only: true` and `max_cost_dollars` ≤ 10"

Cedar either allows or denies — no probabilistic judgment. If a policy denies, the
pipeline stops before a single byte is scanned.

**Bedrock Guardrails (semantic, ML-based)** — applied at LLM I/O (the `plan` tool) and
via the `ApplyGuardrail` API directly on data (probe samples, excavation results, export
payloads, refine summaries). Catches things Cedar can't: a query result that technically
passes the column allowlist but whose combination of fields reconstructs PII, or a result
summary that contains injection-style content from the data itself.

## Proactive Intelligence

Beyond reactive query-and-answer, clAWS runs scheduled watches that surface findings
before anyone asks.

**Five watch types:**

| Watch Type | What it monitors | Use case |
|------------|-----------------|----------|
| `new_award` | NIH Reporter / NSF Awards | Alert when new grants match a lab profile; semantic similarity scoring against stored abstract |
| `literature` | PubMed / bioRxiv rows | Flag papers relevant to a PI's work; classify by reagent/protocol/methodology relevance |
| `cross_discipline` | Adjacent-field papers | Detect papers in other fields addressing your open problems; qualify on cross-field score + citation patterns |
| `compliance` | Institutional data | Evaluate rules for international sites, new data sources, subject counts, classification changes |
| `accreditation` | Evidence against standards | Evaluate evidence predicates per SACSCOC/HLC standard; surface gaps automatically |

**Watch infrastructure:**
- Watches lock the plan at creation — the runner executes it verbatim on schedule. No LLM at execution time.
- **Action routing**: findings can dispatch to SNS, EventBridge, or Bedrock Agent destinations
- Router `summarize` drafts briefing text for each finding (fail-open)
- Findings auto-remember to institutional memory by default (literature/cross_discipline)
- **One-shot flow trigger**: create EventBridge Scheduler `at()` schedules targeting Quick Flows automation

## Institutional Memory

clAWS persists findings across sessions so institutional knowledge accumulates.

- `remember` — append NDJSON to versioned S3 with ETag conditional writes (3 retries on conflict); first write auto-registers as a QuickSight dataset via the Data extension's `register-memory-source` Lambda
- `recall` — filter pipeline: `expires_at` > now → `since_days` → `severity` → `tags` any-match → `query` substring
- Watch runners auto-remember findings by default
- Memory records are queryable as QuickSight datasets for dashboarding

## Compliance Features

**IRB Workflow** — `requires_irb: true` on a plan sets status to `pending_approval`.
Excavation blocked until an authorized reviewer approves. Self-approval blocked.
EventBridge audit event on approval. Watch runner also blocks pending_approval and
template plans.

**FERPA Guardrail Preset** — blocks five student-data categories + SSN/student-ID regex;
deploy with `enable_ferpa_guardrail: true` CDK context.

**Cedar Policy Templates** — four pre-built:
- `read-only.cedar` — metadata-only access; no excavate or export
- `no-pii-export.cedar` — allows excavation but forbids PII export
- `approved-domains-only.cedar` — locks principals to a pre-approved domain list
- `phi-approved.cedar` — PHI access with clearance level ≥ 3, IRB approval, and HITL token

**Per-Principal Budget Caps** — SSM-based limits (`/quick-suite/claws/budget/{principal_arn}`),
DynamoDB monthly spend tracking, 402 on exceeded, fail-open on errors.

**Compliance Audit Export** — HMAC-SHA-256-hashed NDJSON records to S3; keyed secret in
Secrets Manager; fields: `principal`, `tool`, `inputs_hash`, `outputs_hash`, `cost_usd`,
`guardrail_trace`, `timestamp`.

**Export Destination Allowlist** — `CLAWS_EXPORT_ALLOWED_DESTINATIONS` restricts where
data can be exported; HTTPS enforced on all callback destinations.

## Security

- **Column-level access control**: `plan` filters schema by principal roles; `excavate` post-filters result columns
- **Source ID validation**: blocks path traversal, null bytes, control chars, unknown prefixes at handler entry
- **OpenSearch script injection**: recursive walk rejects `script`, `scripted_metric`, `scripted_sort` at any nesting depth
- **Mutation detection**: all six executors (Athena, OpenSearch, S3 Select, DynamoDB PartiQL, PostgreSQL, Redshift) reject INSERT/UPDATE/DELETE/DROP/CREATE/TRUNCATE/ALTER
- **MCP source ID validation**: `plan` validates server name against MCP registry
- **Error sanitization**: no endpoint URLs, index names, or exception details in user-facing responses
- **DynamoDB protection**: PITR + deletion protection on all tables
- **Lambda log retention**: 90-day retention on all Lambda functions
- **Athena IAM**: scoped to `claws-readonly` workgroup ARN (no wildcard)
- **PostgreSQL sessions**: read-only with 60-second statement timeout

## Requirements

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (package manager)
- Node.js 18+ and AWS CDK v2: `npm install -g aws-cdk`
- AWS account with Bedrock model access enabled for your region
- AWS credentials configured (`aws configure` or an IAM role)

## Quick Start

```bash
git clone https://github.com/scttfrdmn/quick-suite-claws.git
cd quick-suite-claws
uv sync --extra dev --extra cdk

cd infra/cdk
cdk deploy --all
```

CDK deploys six stacks in dependency order:

| Stack | What it creates |
|-------|----------------|
| `ClawsStorageStack` | S3 buckets (runs, Athena results, memory), DynamoDB tables (plans, schemas, lookup, spend), all with PITR + deletion protection |
| `ClawsGuardrailsStack` | Bedrock Guardrail with content filters, PII detection, injection blocking; optional FERPA preset |
| `ClawsToolsStack` | 13 Lambda functions, shared IAM role, Athena workgroup |
| `ClawsSchedulerStack` | EventBridge Scheduler for watches, watch runner Lambda, memory + flow trigger IAM |
| `ClawsGatewayStack` | AgentCore Gateway with one Lambda target per tool |
| `ClawsPolicyStack` | Cedar policy deployment and gateway association |

## Capstone Deployment (Shared Gateway)

When deploying alongside the other Quick Suite extensions:

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

Tag your Glue tables so `discover` can find them:

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

## Development and Testing

```bash
# Run the full test suite (455 tests, no AWS credentials required)
uv run pytest tools/ -v

# Lint and format
uv run ruff check tools/
uv run ruff format tools/

# Type check
uv run mypy tools/
```

For live integration tests against real AWS resources:

```bash
export CLAWS_TEST_ATHENA_DB=your_db
export CLAWS_TEST_ATHENA_TABLE=your_table
export CLAWS_TEST_ATHENA_OUTPUT=s3://your-bucket/results/
export CLAWS_TEST_RUNS_BUCKET=your-claws-runs-bucket
uv run pytest tools/tests/live/ -v -m live
```

## Cost

clAWS itself has minimal infrastructure cost — under $5/month at idle. The meaningful
cost is query execution charges: Athena ($5/TB scanned, with partition pruning), Redshift
(standard Data API pricing), PostgreSQL (connection time). Cedar cost limits and per-principal
budget caps keep spend in check.

## Documentation

| Doc | What it covers |
|-----|---------------|
| [docs/getting-started.md](docs/getting-started.md) | Deploy and run your first excavation, step by step |
| [docs/user-guide.md](docs/user-guide.md) | Tool reference, Cedar policy authoring, guardrail customization, team and IRB workflows |
| [docs/compliance.md](docs/compliance.md) | IRB workflow, FERPA preset, Cedar policy templates, audit export |
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
