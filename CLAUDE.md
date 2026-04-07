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
                                               ↓
                                        watch / watches  (v0.7+, persistence)
```

- `discover` — find sources in approved domains (Glue catalog search)
- `probe` — inspect schema, samples, cost estimates
- `plan` — translate free-text objective → concrete query (LLM + Guardrails)
- `excavate` — execute concrete query from plan (Athena/OpenSearch/S3 Select/MCP)
- `refine` — dedupe, rank, summarize results
- `export` — materialize to S3/EventBridge with provenance
- `watch` *(v0.7)* — create/update/delete a scheduled watch on a locked plan
- `watches` *(v0.7)* — list active watches and last-run status

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
│   │   └── executors/            # athena.py, opensearch.py, s3_select.py, mcp.py
│   ├── mcp/                      # MCP registry + async client bridge
│   │   ├── registry.py           # Server config from env/S3
│   │   └── client.py             # asyncio.run() bridge for Lambda
│   ├── refine/handler.py         # Dedupe, rank, summarize with grounding
│   ├── export/handler.py         # S3/EventBridge export with provenance
│   ├── watch/                    # (v0.7) Scheduled watch CRUD + runner
│   ├── watches/                  # (v0.7) List/status tool
│   └── tests/
│       ├── test_pipeline.py      # E2E integration tests
│       ├── test_shared.py
│       └── live/                 # Manual live-AWS tests (pytest -m live)
├── infra/cdk/                    # Python CDK stacks
│   ├── app.py
│   └── stacks/                   # storage, tools, gateway, guardrails, policy
│                                 # scheduler_stack.py (v0.7)
├── docs/
│   ├── architecture.md
│   ├── safety-model.md
│   ├── getting-started.md
│   ├── user-guide.md
│   ├── mcp-integration.md
│   ├── capstone-deployment.md
│   └── quick-suite-integration.md
├── examples/                     # genomics, log-analysis, document-mining
├── CONTRIBUTING.md
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
# Run all tests — 209 passing, no AWS credentials required
uv run pytest tools/ -v

# Lint and format
uv run ruff check tools/
uv run ruff format tools/

# Type check
uv run mypy tools/
```

Three test tiers:

1. **Pure unit** — `sql_validator`, `cost_estimator`, `test_shared`: no AWS, no fixtures
2. **Substrate integration** — all handler and executor tests: real S3/DynamoDB/Glue/Athena
   via the `substrate` fixture (pytest-substrate). OpenSearch, Bedrock, EventBridge, and MCP
   are mocked with `MagicMock` (substrate OpenSearch support tracked in
   [scttfrdmn/substrate#253](https://github.com/scttfrdmn/substrate/issues/253))
3. **Live AWS** — `tools/tests/live/`: manual only, requires `CLAWS_TEST_*` env vars,
   run with `uv run pytest tools/tests/live/ -m live`

Tests never use `moto`. All AWS mocking goes through substrate or `MagicMock`.

## Dependencies

```bash
uv sync --extra dev          # Dev: pytest, ruff, mypy, pytest-substrate
uv sync --extra cdk          # CDK: aws-cdk-lib, constructs
uv sync --extra dev --extra cdk   # Both
```

Runtime dependencies: `boto3`, `mcp>=1.0`, `opensearch-py`, `requests-aws4auth`.
`pytest-substrate` is a local file reference (`file:///Users/scttfrdmn/src/substrate/python`);
requires substrate server >=0.45.3.

Config in `pyproject.toml`. Line length 100. Target Python 3.12.

## What's done

- Full OpenAPI spec for all 6 tools
- Cedar policies (default + research-team + restricted-dataset examples)
- Bedrock Guardrail configs (base + genomics + financial tenant overlays)
- All 6 tool handlers with real implementation logic
- All 4 executors: Athena, OpenSearch (with aggregation flattening), S3 Select, MCP
- MCP registry (inline JSON or S3 URI config) + asyncio.run() client bridge
- SQL validator with mutation detection, multi-statement blocking
- Cost estimator with Athena pricing model, partition pruning heuristics
- Guardrail scanning (ApplyGuardrail API integration in shared.py)
- Plan-to-execution linkage (plan_id validation in excavate)
- CDK stacks for all infrastructure, including Capstone mode (shared Gateway)
- Live AWS test tier (`tools/tests/live/`) for manual pre-release validation
- v0.10.0 collaboration features: `team_id` on plans + watches; `claws.team_plans`; `claws.share_plan`; `excavate` shared_with check; catalog-aware `discover` with `registry` domain querying `qs-data-source-registry`
- v0.11.0 compliance: IRB `pending_approval` plan status; `approve_plan` internal Lambda; `plan.approve` Cedar action; FERPA Guardrail preset (`guardrails/ferpa/ferpa_guardrail.json`); four Cedar policy templates (`policies/templates/`); compliance audit export Lambda (`audit_export`) writing SHA-256-hashed NDJSON; CDK `enable_ferpa_guardrail` context flag
- v0.12.0 security: column-level access control (#61); multi-backend cost estimator (#62); HMAC-SHA-256 audit hashing (#73); MCP source ID validation (#74); mutation detection in DynamoDB + S3 Select executors (#81); refine summary guardrail scan (#82); `requires_irb` enforcement in `approve_plan` (#86); OpenSearch error sanitization (#87); DynamoDB PITR + deletion protection on all tables (#83); Lambda log retention policies (#84); Athena IAM scoped to workgroup ARN (#85)
- v0.13.0 security: silent guardrail bypass made visible (`bypassed` status + ERROR log) (#77); `validate_source_id()` at handler entry points blocks path traversal and unknown prefixes (#78); OpenSearch DSL script injection blocked via `_check_dsl_scripts()` recursive walk (#76); Cedar `plan.approve` permit requires `requires_irb == true` + `status == "pending_approval"` (#75)
- v0.14.0 features: plan templating with `{{variable}}` substitution via `instantiate_plan` tool (#66); export destination URI allowlist (`CLAWS_EXPORT_ALLOWED_DESTINATIONS`) + HTTPS enforcement on callback destinations (#80); watch runner plan status check blocks `pending_approval`/`template` plans at execution time (#79)
- 209 passing tests (substrate integration + pure unit)

## Work tracking

Work is tracked in GitHub — see milestones and issues at
https://github.com/scttfrdmn/claws/milestones

Released:
- **v0.4.1** — OpenSearch aggregation flattening + executor tests ✓
- **v0.5.0** — MCP extensibility (issues #22–#27) ✓
- **v0.6.0** — Capstone integration (issues #33–#34) ✓
- **v0.7.0** — Scheduled watches: `claws.watch` + `claws.watches` tools, watch runner Lambda, EventBridge Scheduler, `ClawsSchedulerStack`, DynamoDB PartiQL executor (issues #35–#40, #49) ✓
- **v0.8.0** — Feed materialization: `merge` refine operation, feed watch type, export append/overwrite mode (issues #41–#44) ✓
- **v0.9.0** — Drift detection: `diff_results` utility in `shared.py`, drift condition type in watch runner, diff summary passthrough in export provenance (issues #45–#48) ✓
- **v0.10.0** — Collaboration: `team_id` on plans + watches, `claws.team_plans` tool, `claws.share_plan` tool, catalog-aware `discover` querying quick-suite-data source registry (issues #51–#55) ✓
- **v0.11.0** — Compliance: IRB approval workflow (`pending_approval` plan status + `approve_plan` Lambda + `plan.approve` Cedar action), FERPA Guardrail preset, four Cedar policy templates, compliance `audit_export` Lambda (NDJSON with SHA-256 hashed I/O) (issues #56–#60) ✓
- **v0.12.0** — Security hardening: column-level access control, multi-backend cost estimator, HMAC audit hashing, MCP source validation (#61, #62, #73, #74); mutation detection in DynamoDB + S3 Select, refine summary guardrail scan, `requires_irb` enforcement, OpenSearch error sanitization, DynamoDB PITR + deletion protection, Lambda log retention, Athena IAM scoped to workgroup (issues #81–#87) ✓
- **v0.13.0** — P1 security fixes: silent guardrail bypass now visible (#77), source_id validation at handler entry (#78), OpenSearch DSL script injection blocked (#76), Cedar plan.approve requires requires_irb + pending_approval (#75) ✓
- **v0.14.0** — Plan templating with `{{variable}}` placeholders + `instantiate_plan` tool (#66); export destination allowlist + HTTPS enforcement (#80); watch runner plan status check (#79) ✓

All four roadmap themes complete: Safety (v0.4–v0.6), Extensibility (v0.7–v0.8), Observability (v0.9), and Compliance (v0.11–v0.13).

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
7. **Watches lock the plan at creation** — the watch runner executes the stored
   plan verbatim on every scheduled run. No LLM is invoked at execution time.
   This is strictly safer than on-demand: the query is immutable and auditable
   for the lifetime of the watch.
