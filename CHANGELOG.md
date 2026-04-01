# Changelog

All notable changes to clAWS will be documented here.

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning: [Semantic Versioning 2.0.0](https://semver.org/).

## [Unreleased]

## [0.2.0] - 2026-04-01

### Added
- `tools/excavate/executors/opensearch.py`: OpenSearch DSL executor with SigV4 signing via `requests-aws4auth`; per-endpoint client cache; `max_rows` capped at 1000; timeout detection; JSON-string or dict query input (#11)
- `tools/shared.py`: `emit_metric()` for CloudWatch custom metrics; `cloudwatch_client()` singleton; `CLAWS_METRICS_NAMESPACE` env var guard; metrics emitted from `audit_log()` — Invocations, Errors, GuardrailBlocks, Timeouts, CostDollars, RowsReturned (#13)
- `request_id` propagation: extracted from Lambda `requestContext.requestId` in all 6 handlers and included in `audit_log()` JSON output (#12)
- CDK `ClawsGatewayStack`: `AwsCustomResource` creates AgentCore Gateway (`createAgentRuntime`), registers each tool Lambda as an endpoint (`createAgentRuntimeEndpoint`), grants `bedrock-agentcore.amazonaws.com` invoke permission (#14)
- CDK `ClawsPolicyStack`: `AwsCustomResource` deploys `policies/default.cedar` as a Cedar policy (`createPolicy`), associates it with the gateway (`associateAgentRuntimeWithPolicy`) (#14)
- CDK `ClawsToolsStack`: `cloudwatch:PutMetricData` IAM policy (namespace-scoped to `claws`); `CLAWS_METRICS_NAMESPACE=claws` Lambda environment variable (#13, #14)
- Runtime dependencies: `opensearch-py>=2.4`, `requests-aws4auth>=1.2`, `requests>=2.31`
- 19 new tests (71 total): `TestEmitMetric` (4), `TestAuditLogMetrics` (5), `TestOpenSearchExecutor` (10)

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

[Unreleased]: https://github.com/scttfrdmn/claws/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/scttfrdmn/claws/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/scttfrdmn/claws/releases/tag/v0.1.0
