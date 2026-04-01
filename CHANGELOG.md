# Changelog

All notable changes to clAWS will be documented here.

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning: [Semantic Versioning 2.0.0](https://semver.org/).

## [Unreleased]

## [0.1.0] - 2026-03-31

### Added
- Six Lambda tool handlers: `discover`, `probe`, `plan`, `excavate`, `refine`, `export`
- `discover`: Glue Data Catalog search with space-based scoping and relevance scoring
- `probe`: Athena table schema inspection via Glue, size estimates, PII-scanned samples; `_sample_athena()` for live row sampling
- `plan`: Free-text objective → concrete SQL/DSL query via Bedrock with Guardrails attachment; SQL validator (mutation detection, multi-statement blocking); cost estimator with Athena pricing and partition pruning heuristics
- `excavate`: Plan-linked execution with `plan_id` bait-and-switch protection; Athena executor (read-only workgroup enforcement, paginated results); S3 Select executor (CSV/JSON/Parquet, pure boto3)
- `refine`: Dedupe, rank, normalize, summarize operations with Bedrock grounding check
- `export`: S3 export with provenance chain; EventBridge/callback stubs
- `tools/errors.py`: Typed exception hierarchy (`ClawsError`, `ValidationError`, `ForbiddenError`, `NotFoundError`, `ExecutionError`, `UpstreamError`, `GuardrailBlockedError`)
- `tools/shared.py`: Audit logging, result/plan/schema storage helpers, guardrail scanning via `ApplyGuardrail` API, Lambda response helpers
- Cedar policies: `default.cedar`, `research-team` and `restricted-dataset` examples
- Bedrock Guardrail configs: base config (content filters, PII/PHI, injection detection); genomics and financial tenant overlays
- CDK stacks: storage, tools, guardrails (gateway and policy stacks are CLI-configured placeholders pending AgentCore CDK construct availability)
- Full OpenAPI spec (`api/openapi.yaml`) for all 6 tools
- Handler-level moto tests for all 6 tools (~32 tests)
- Architecture, safety model, and Quick Suite integration design docs
- Example workflows: genomics excavation, log analysis, document mining

[Unreleased]: https://github.com/scttfrdmn/claws/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/scttfrdmn/claws/releases/tag/v0.1.0
