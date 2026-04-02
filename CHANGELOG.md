# Changelog

All notable changes to clAWS will be documented here.

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning: [Semantic Versioning 2.0.0](https://semver.org/).

## [Unreleased]

## [0.7.0] - 2026-04-02

### Added
- `claws.watch` tool Lambda: create, update, and delete scheduled watches on locked plans; schedule validated as `rate()` or `cron()` expression; plan must exist at creation time (422 if not found) (#35)
- `claws.watches` tool Lambda: list watches with optional `status_filter` and `source_id_filter`; `source_id` denormalized from plan at watch creation time (#36)
- `claws-watches` DynamoDB table in `ClawsStorageStack`: PK `watch_id`, 90-day default TTL (#37)
- Watch runner Lambda (`claws-watch-runner`): receives `watch_id` from EventBridge Scheduler, executes locked plan, evaluates optional condition (`gt`/`gte`/`lt`/`lte`/`eq`/`ne`), fires notification target; no LLM at execution time; increments `consecutive_errors` and pauses watch after 3 failures (#38)
- `ClawsSchedulerStack`: EventBridge ScheduleGroup `claws-watches`, runner Lambda with Athena/S3/DynamoDB/PartiQL IAM grants, scheduler execution role; wires `CLAWS_WATCH_RUNNER_ARN` and `CLAWS_WATCH_RUNNER_ROLE_ARN` into watch tool Lambda (#39)
- DynamoDB PartiQL executor (`tools/excavate/executors/dynamodb.py`): paginates `execute_statement()` up to 1000 rows; `dynamodb_partiql` query type added to `EXECUTORS` dispatch (#49)
- `store_watch()`, `load_watch()`, `update_watch()`, `delete_watch()`, `list_watches()`, `new_watch_id()` in `tools/shared.py`
- `_clean_item()` utility in `tools/shared.py`: strips `None` and empty collections before DynamoDB writes (required for Substrate compatibility)
- 15 new tests: 6 watch handler (create/update/delete), 3 watches handler (list/filter), 6 runner (condition eval, audit log, last-run tracking, error handling) (#40)

## [0.6.1] - 2026-04-02

### Added
- `store_result_metadata()` in `tools/shared.py`: writes `result_metadata.json` alongside `result.json` in S3; fields: `run_id`, `schema`, `row_count`, `bytes_scanned`, `cost`, `source_id`, `created_at`
- Schema inferred from first result row: `int→bigint`, `float→double`, `bool→boolean`, other→`string`; empty result sets produce `schema: []`
- `metadata_uri` field in `excavate` response pointing to the metadata file
- 3 new tests: metadata written alongside result, schema type inference, empty-row graceful handling (#29)

## [0.6.0] - 2026-04-02

### Added
- CDK Capstone mode: `ClawsGatewayStack` optional `shared_gateway_id` context var to register clAWS tools on an existing AgentCore Gateway (shared with Router/Data/Compute); standalone and shared modes both supported (#34)
- `TestFullPipeline` integration test class: 10 end-to-end tests covering the full `plan → excavate → refine → export` chain using Substrate; validates plan_id bait-and-switch protection at handler boundary (#33)
- `docs/capstone-deployment.md`: shared-Gateway deployment guide for the Quick Suite capstone integration

## [0.5.0] - 2026-04-02

### Added
- MCP extensibility: `tools/mcp/registry.py` loads server config from env or S3; `tools/mcp/client.py` provides `asyncio.run()` bridge for Lambda sync handlers (#22)
- `tools/excavate/executors/mcp.py`: `execute_mcp()` executor + content-block adapter; `mcp_tool` query type added to `EXECUTORS` dispatch (#23)
- `discover._discover_mcp()`: discovers MCP server tools as data sources; `"mcp"` domain added to discover routing (#24)
- `probe._probe_mcp()`: invokes MCP tools to probe schema and samples (#25)
- `plan/handler.py`: `mcp_tool` query type; bypasses SQL validator and cost estimator for MCP plans (#26)
- `docs/mcp-integration.md`: MCP server configuration, tool discovery, and excavation guide (#27)
- 50 new tests covering MCP executor, registry, and all handler MCP paths

## [0.4.1] - 2026-04-01

### Added
- `execute_opensearch()`: aggregation result flattening — `aggs` response keys merged into each hit row; `bucket_key` and `doc_count` exposed as columns (#9)
- `constraints.read_only` enforcement in Athena executor: sets `workgroup` to `"claws-read-only"` when `read_only: true` (#10)
- 10 new executor tests in `test_executors.py` covering OpenSearch aggregation flattening and Athena read_only workgroup enforcement

## [0.4.0] - 2026-04-01

### Added
- `export._export_to_eventbridge()`: publishes results to an EventBridge event bus; URI format `events://bus-name/detail-type`; checks `FailedEntryCount`; `EVENTS_CLIENT` singleton + `_events_client()` lazy init (#20)
- `export._export_to_callback()`: POSTs results to an HTTPS callback URL; optional HMAC-SHA256 `X-Claws-Signature` header when `CLAWS_CALLBACK_SECRET` env var is set (#21)
- CDK `ClawsToolsStack`: `events:PutEvents` IAM policy for EventBridge export destination (#20)
- 6 new tests (105 total): `TestEventBridgeExport` (3), `TestCallbackExport` (3)

## [0.3.0] - 2026-04-01

### Added
- `discover._discover_opensearch()`: lists OpenSearch domain indices via `cat.indices`; scores by query-term match in index name; reuses `_os_client()` from the excavate executor (#15)
- `discover._discover_s3()`: lists S3 bucket common prefixes and object keys via `list_objects_v2`; scores by query-term match; `_s3_client()` singleton added (#15)
- `probe._probe_opensearch()`: index mapping → schema columns, index stats → row/size estimates, optional sample documents via `match_all`; reuses `_os_client()` and `_parse_source_id()` from excavate executor (#16)
- `refine._filter()`: parameterized row filtering with operators `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `contains`, `not_contains`; operations list now accepts dict entries `{"op": "filter", "field": ..., "operator": ..., "value": ...}` alongside existing string ops (#17)
- `tools/excavate/tests/test_executors.py`: 13 direct unit tests for `execute_athena()` (5 tests: complete, failed, timeout, start-failure, cost calculation) and `execute_s3_select()` (8 tests: source-id parsing, format detection, CSV execute, missing key) (#18)
- `tools/tests/test_pipeline.py`: 3 end-to-end tests validating plan_id bait-and-switch protection across plan→excavate handler boundary (#19)

### Fixed
- `refine._filter()`: rows where the filtered field is absent are preserved (graceful no-op per row)

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

[Unreleased]: https://github.com/scttfrdmn/quick-suite-claws/compare/v0.7.0...HEAD
[0.7.0]: https://github.com/scttfrdmn/quick-suite-claws/compare/v0.6.1...v0.7.0
[0.6.1]: https://github.com/scttfrdmn/quick-suite-claws/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/scttfrdmn/quick-suite-claws/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/scttfrdmn/quick-suite-claws/compare/v0.4.1...v0.5.0
[0.4.1]: https://github.com/scttfrdmn/quick-suite-claws/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/scttfrdmn/quick-suite-claws/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/scttfrdmn/quick-suite-claws/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/scttfrdmn/quick-suite-claws/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/scttfrdmn/quick-suite-claws/releases/tag/v0.1.0
