# CLAUDE.md — clAWS project context for Claude Code

## Project management

- Versioning: **semver2** (semantic versioning 2.0.0)
- Changelog: **Keep a Changelog** format (`CHANGELOG.md`)
- Work tracking: **GitHub** — projects, milestones, issues, labels
- Do **not** use CLAUDE.md or standalone files for tracking work items

## What this is

clAWS is an open-source reference architecture for building enterprise-grade
data excavation tool planes that agents can call safely. It runs on Amazon
Bedrock AgentCore with Cedar policies for structural enforcement and Bedrock
Guardrails for content safety. Amazon Quick Suite is an optional operator surface.

Apache 2.0. Python 3.12+. CDK for infra.

## Architecture sentence

clAWS is a secure excavation tool plane on Amazon Bedrock AgentCore,
with Cedar policies for structural enforcement, Bedrock Guardrails for
content safety, and Amazon Quick Suite as an optional operator and
workflow surface.

## Core design principle

**LLM reasoning never happens inside a tool.** Tools are deterministic
executors. The `plan` tool is the ONLY tool that accepts free-text input.
It returns a concrete, reviewable execution plan (the actual SQL, cost
estimate, output schema). The `excavate` tool takes that plan verbatim.
Cedar policies gate the concrete query, not the intent.

## Tool pipeline

```
discover → probe → plan → excavate → refine → export
```

- `discover` — find sources in approved domains (Glue catalog search)
- `probe` — inspect schema, samples, cost estimates
- `plan` — translate free-text objective → concrete query (LLM + Guardrails)
- `excavate` — execute concrete query from plan (Athena/OpenSearch/S3 Select)
- `refine` — dedupe, rank, summarize results
- `export` — materialize to S3/EventBridge with provenance

## Safety layers

Two independent enforcement layers:
1. **Cedar (AgentCore Policy)** — structural/deterministic at Gateway boundary
2. **Bedrock Guardrails** — semantic/content at LLM I/O and data paths

Cedar gates structure (permissions, bounds, allowlists). Guardrails gates
content (PII, injection, denied topics, grounding). Neither alone is sufficient.

Guardrails integration modes:
- **Model guardrail** on `InvokeModel` calls in plan and refine
- **`ApplyGuardrail` API** for standalone content scanning on probe samples,
  excavate results, and export payloads

## Project layout

```
claws/
├── api/openapi.yaml              # Gateway-facing tool definitions (complete)
├── policies/                     # Cedar policies
│   ├── default.cedar             # Base policies for all principals
│   └── examples/                 # Domain-specific policy examples
├── guardrails/                   # Bedrock Guardrail configs
│   ├── base/                     # Shared: content-filters, pii, injection
│   └── tenants/                  # Per-tenant overlays
├── tools/                        # Lambda handlers
│   ├── shared.py                 # Utilities, audit, guardrail scanning
│   ├── discover/handler.py       # Glue catalog search
│   ├── probe/handler.py          # Schema + samples with PII scan
│   ├── plan/                     # LLM query generation
│   │   ├── handler.py            # Bedrock invocation with guardrail
│   │   └── validators/           # sql_validator.py, cost_estimator.py
│   ├── excavate/                 # Query execution
│   │   ├── handler.py            # Plan-linked execution + result scanning
│   │   └── executors/            # athena.py (done), opensearch.py, s3_select.py (stubs)
│   ├── refine/handler.py         # Dedupe, rank, summarize with grounding
│   └── export/handler.py         # S3 export with provenance + final scan
├── infra/cdk/                    # Python CDK stacks
│   ├── app.py
│   └── stacks/                   # storage, tools, gateway, guardrails, policy
├── docs/                         # architecture, safety-model, quick-suite
├── examples/                     # genomics, log-analysis, document-mining
└── pyproject.toml                # Project config, deps, pytest, ruff, mypy
```

## Code conventions

- Python 3.12+, type hints everywhere
- `pyproject.toml` for all config (no setup.py, no requirements.txt at root)
- CDK stacks use Python CDK (not TypeScript) — consistent with QuickSuite repos
- Tool handlers follow the Lambda handler pattern: `handler(event, context) -> dict`
- All handlers parse body from `event.get("body")` or directly from `event`
- Shared utilities in `tools/shared.py` — audit logging, S3/DynamoDB helpers,
  guardrail scanning
- ID generation: `plan-{hex8}`, `run-{hex8}`, `export-{hex8}`
- Cedar policies in `.cedar` files, guardrail configs in `.json`
- OpenAPI spec in `api/openapi.yaml` is the source of truth for tool schemas

## Testing

```bash
# Run all tests (pure logic, no AWS deps needed)
pytest tools/ -v

# Current test coverage
# tools/plan/tests/test_sql_validator.py — 13 tests (mutation detection, multi-statement, etc.)
# tools/plan/tests/test_cost_estimator.py — 4 tests (Athena pricing, partition pruning)
```

Tests use pytest. For tests that need AWS services, use moto for mocking
(listed in dev dependencies). The sql_validator and cost_estimator tests
are pure logic and run without any mocking.

## Dependencies

```bash
pip install -e ".[dev]"     # Dev: pytest, ruff, mypy, moto
pip install -e ".[cdk]"     # CDK: aws-cdk-lib, constructs
```

Runtime dependency is just `boto3`.

## Linting and formatting

```bash
ruff check tools/           # Lint
ruff format tools/           # Format
mypy tools/                  # Type check
```

Config in `pyproject.toml`. Line length 100. Target Python 3.12.

## What's done

- Full OpenAPI spec for all 6 tools
- Cedar policies (default + research-team + restricted-dataset examples)
- Bedrock Guardrail configs (base + genomics + financial tenant overlays)
- All 6 tool handlers with real implementation logic
- All 4 executors: Athena, OpenSearch (with aggregation flattening), S3 Select, MCP
- SQL validator with mutation detection, multi-statement blocking
- Cost estimator with Athena pricing model, partition pruning heuristics
- Guardrail scanning (ApplyGuardrail API integration in shared.py)
- Plan-to-execution linkage (plan_id validation in excavate)
- CDK stacks for all infrastructure
- 145 passing tests

## Work tracking

Work is tracked in GitHub — see milestones and issues at
https://github.com/scttfrdmn/claws/milestones

Current milestones:
- **v0.4.1** — OpenSearch aggregation flattening + executor tests
- **v0.5.0** — MCP extensibility (issues #22–#27)
- **v0.6.0** — Capstone integration (issues #28–#29)

## Design docs

- `docs/architecture.md` — component map, tool pipeline, storage, deployment
- `docs/safety-model.md` — Cedar vs Guardrails, attachment points, defense in depth
- `docs/quick-suite-integration.md` — Flows, Automate, HITL, dashboards

Full design document with all API examples and policy details is in the
conversation history where this project was created.

## Key design decisions to preserve

1. **plan/execute split** — plan is the ONLY tool with free-text input. excavate
   takes concrete queries. This is the core safety property.
2. **plan_id linkage** — excavate validates the submitted query matches the stored
   plan. Prevents bait-and-switch after approval.
3. **Cedar at Gateway, Guardrails at content** — two independent layers. Don't
   collapse them.
4. **No guardrail on discover** — structured I/O, metadata only. Adding one
   would cost latency for zero safety gain.
5. **Quick Suite is optional** — clAWS runs entirely on AgentCore. Quick Suite
   connects through the same Gateway as any other client.
6. **ApplyGuardrail for data paths** — result scanning uses the standalone API,
   not model guardrails. This scans data without LLM involvement.
