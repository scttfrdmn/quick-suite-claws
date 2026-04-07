# Changelog

All notable changes to clAWS will be documented here.

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning: [Semantic Versioning 2.0.0](https://semver.org/).

## [Unreleased]

## [0.16.0] - 2026-04-07

### Added
- **Issue #71: Living literature watch (`watch_type: "literature"`)** — New watch type that monitors PubMed/bioRxiv excavation results for papers semantically relevant to a lab research profile. Watch spec requires `semantic_match.lab_profile_ssm_key` (same pattern as `new_award`). Optional `reagent_config_uri` (S3/SSM JSON list of reagent keywords) and `protocol_config_uri` (S3/SSM JSON list of method names) enable per-paper `relevance_type` classification: `"reagent"` (keywords found → `validation_steps: ["confirm_antibody_catalog_number"]`), `"protocol"` (method matched → `validation_steps: ["replicate_protocol"]`), or `"methodology"` (default → `validation_steps: ["cite_and_review"]`). Runner calls Router `summarize` per paper (cap 50 rows); papers scoring ≥ `abstract_similarity_threshold` (default 0.75) are returned sorted by score descending. Router failures are non-blocking.
- **Issue #72: Cross-discipline signal watch (`watch_type: "cross_discipline"`)** — New watch type that detects papers from adjacent research fields addressing open problems in a primary domain. Watch spec requires `open_problems_uri` (S3/SSM URI to JSON list of `{gap_statement, domain}` objects) and `primary_field` (string). Optional `field_distance` (default 0.5) and `citations_in_primary_field` (default 5) control how cross-field a paper must be. Runner loads the open-problems list, calls Router `research` with `grounding_mode="strict"` per paper, parses the JSON response for `cross_field_score`, `source_field`, and `citations_in_primary_field`; papers meeting both thresholds are returned with `{gap_id, gap_statement, source_field, cross_field_score}` appended.
- **`call_router()` grounding mode parameter** — `tools/shared.py`: `call_router()` now accepts an optional `grounding_mode` parameter (default `"default"`); when set to `"strict"`, the field is included in the router request body so the research tool activates citation-grounded mode.
- **Two Cedar permits in `policies/default.cedar`** — `claws.literature_watch` and `claws.cross_discipline_watch` actions permitted for principals with `lab_director` role.

### Tests
- 25 new tests in `tools/tests/test_v16_watches.py`: `TestLiteratureWatchValidation` (3) — missing semantic_match, missing SSM key, valid spec with optional URI fields stored; `TestRunLiteratureWatch` (8) — happy path with relevance_type, threshold filter, reagent/protocol type detection, Router failure non-blocking, empty rows, SSM failure, sort order; `TestCrossDisciplineWatchValidation` (3) — missing open_problems_uri, missing primary_field, valid spec; `TestRunCrossDisciplineWatch` (7) — qualifying paper with gap metadata, field_distance filter, high-citations filter, SSM URI dispatch, Router failure skip, empty rows, URI load failure; `TestCallRouterGroundingMode` (2) — strict mode in request body, default mode omitted.

## [0.15.0] - 2026-04-07

### Added
- **New-award intelligence watch (#70):** New `watch_type: "new_award"` executes a locked plan against NIH Reporter or NSF Awards source data, fetches a lab profile abstract from SSM Parameter Store, and scores each award abstract for semantic similarity via Router `summarize`; only awards meeting or exceeding `abstract_similarity_threshold` (default 0.82) are included in the notification payload; Router failures are non-blocking (logged as warnings, award skipped); maximum 50 rows scored per run to cap Router spend
- **Discover domain allowlist for research APIs (#70):** `nih-reporter` and `nsf-awards` domain strings in `discover` now dispatch to `_discover_registry()` with the corresponding `source_type_filter`; supports scoped discovery against quick-suite-data registry entries of type `nih_reporter` and `nsf_awards`
- **SSM read permission in scheduler CDK:** Watch runner IAM role now has `ssm:GetParameter` on `/quick-suite/claws/*` parameter path; required for lab profile fetch at execution time
- 16 new tests in `tools/tests/test_new_award_watch.py` covering watch creation validation, SSM/Router failure modes, 50-row cap, full handler integration, and discover domain dispatch
- **Watch action routing (#68):** New `action_routing` field on any watch type; on trigger, calls Router `summarize` with a `context_template` (substituting `diff_summary` values) to draft a context-specific response; routes draft + affected rows to `destination_arn` via SNS (`sns:Publish`) or EventBridge (`put_events`); Router failures are fail-open (raw delivery without draft); `bedrock_agent` destination logged as warning (not yet supported)
- **Accreditation evidence ledger (#67):** New `accreditation_config_uri` field on watch; config (loaded from `s3://` or `ssm:/` URI) maps standard IDs (e.g., `SACSCOC-8.2.c`, `HLC-4.A.1`) to evidence predicates; runner evaluates predicates against excavation results and returns `accreditation_gaps` list identifying standards with no satisfying evidence; new `claws.accreditation_watch` Cedar action gated on `accreditation_reviewer` role
- **Compliance surface watch (#69):** New `compliance_mode: true` + `compliance_ruleset_uri` fields on watch; ruleset JSON (loaded from S3) defines rules for international sites, new data sources, subject count increases, and data classification changes; runner evaluates rules and generates `compliance_gaps` list with per-gap draft amendment text via Router `summarize`; new `claws.compliance_watch` Cedar action gated on `irb_monitor` role; `load_config_from_uri()` shared utility added to `tools/shared.py` for `s3://` and `ssm:/` JSON config loading
- **SNS publish permission in scheduler CDK:** Watch runner IAM role now has `sns:Publish` on `*` for action_routing SNS destinations
- 26 new tests in `tools/tests/test_v15_completion.py`

## [0.14.0] - 2026-04-07

### Added
- **Plan templating (#66):** `plan` handler accepts `is_template: true` + `template_variables: {var: default}` to store a reusable objective blueprint (e.g., `"Find {{disease}} patients since {{start_date}}"`) without LLM invocation; new `instantiate_plan` AgentCore tool resolves `{{variable}}` placeholders with caller-supplied values and delegates to the standard plan generation flow; `excavate` blocks plans with `status="template"` until instantiated; values containing `{{` are rejected to prevent nested template injection
- **Export destination allowlist (#80):** `CLAWS_EXPORT_ALLOWED_DESTINATIONS` env var (comma-separated URI prefixes) gates all export destinations; `_validate_destination_uri()` in `export/handler.py` checks every export against the list before any I/O; callback destinations always require `https://` regardless of allowlist; unset allowlist preserves backward-compatible allow-all behavior
- 34 new tests in `tools/tests/test_v14_features.py`

### Fixed
- **Watch runner executes non-executable plans (#79):** Watch runner (`tools/watch/runner.py`) now checks `plan["status"]` after loading the plan and before calling the executor; plans with status `"pending_approval"` or `"template"` (or any non-`ready`/`approved` status) cause the watch to be marked errored rather than silently executing; mirrors the identical guard already in `excavate/handler.py`

## [0.13.0] - 2026-04-07

### Fixed
- **Silent guardrail bypass when GUARDRAIL_ID unconfigured (#77):** `apply_guardrail()` now logs at ERROR level and returns `{"bypassed": True}` when `CLAWS_GUARDRAIL_ID` is empty; `scan_payload()` returns `{"status": "bypassed"}` instead of `{"status": "clean"}`; a startup warning is emitted at module load time; all existing callers check for `"blocked"` only and are unaffected
- **source_id not validated before use (#78):** `validate_source_id()` added to `shared.py`; called at the top of `plan/handler.py` and `excavate/handler.py`; rejects empty values, path traversal (`..`), null bytes, control characters, values over 512 characters, and any value not starting with a known prefix (`athena:`, `dynamodb:`, `s3:`, `opensearch:`, `mcp:`, `registry:`)
- **OpenSearch DSL script injection via aggregation body (#76):** `_check_dsl_scripts()` recursively walks the parsed DSL body before execution; `"script"`, `"scripted_metric"`, and `"scripted_sort"` fields at any nesting level (up to depth 20) are rejected with a clear error; Groovy/Painless server-side script execution via aggregation DSL is no longer possible
- **Cedar plan.approve permit clause missing structural conditions (#75):** `policies/default.cedar` `plan.approve` permit now requires `resource.requires_irb == true && resource.status == "pending_approval"` in addition to the `irb_approver` role and self-approval block; Cedar is now authoritative for both the role check and the plan-state preconditions
- 32 new security tests in `tools/tests/test_v14_security.py`

## [0.12.0] - 2026-04-06

### Added
- **Column-level access control (#61):** `plan` filters schema by principal roles (`pii_access`, `phi_cleared`) before query generation; `excavate` post-filters result columns to `allowed_columns` from the plan; principals without clearance receive redacted column sets
- **Multi-backend cost estimator (#62):** `tools/plan/validators/cost_estimator.py` extended with Athena ($0.005/GB scanned), DynamoDB (RCU-based), and MCP (per-request) pricing models; `estimate_cost()` dispatches by `query_type`
- **HMAC-SHA-256 audit hashing (#73):** `audit_export` Lambda uses `_hmac_sha256_of()` + `_sanitise_record()` with a keyed secret from Secrets Manager (`claws/audit-hmac-key`); audit NDJSON contains irreversible hashes of `inputs` and `outputs` — no raw query text or results
- **MCP source ID validation (#74):** `plan` validates that the MCP server name in `source_id` is present in the MCP registry before generating a query; returns a 400 error with available server names if the server is unregistered
- **Mutation detection in DynamoDB and S3 Select executors (#81):** Both `dynamodb.py` and `s3_select.py` now call `_check_mutation()` before executing any query; `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `DROP`, `TRUNCATE`, and `ALTER` statements are rejected with a clear read-only error rather than forwarded to AWS
- **Refine summary guardrail scan (#82):** The LLM-generated summary text returned by `_summarize()` in `tools/refine/handler.py` is now scanned through `ApplyGuardrail` before being returned; blocked summaries are replaced with `"[Summary blocked by content policy]"` rather than silently returned
- **`requires_irb` enforcement in `approve_plan` (#86):** `approve_plan` now checks `plan.requires_irb` before any other approval logic; plans created without `requires_irb: True` cannot be approved through the IRB pathway, closing the gap where a `pending_approval`-status plan could be approved even if it was not marked for IRB review
- 25 new security tests in `tools/tests/test_v13_security.py`

### Fixed
- **DynamoDB tables missing PITR and deletion protection (#83):** `schemas_table` and `watches_table` now have `point_in_time_recovery=True`; all three tables (`plans_table`, `schemas_table`, `watches_table`) have `deletion_protection=True`
- **Lambda log groups missing retention policy (#84):** All 12 Lambda functions (10 tool + 2 internal) now set `log_retention=RetentionDays.THREE_MONTHS`; CloudWatch log groups no longer accumulate indefinitely
- **Athena IAM policy wildcard resource (#85):** `tools_stack.py` Athena policy resources changed from `"*"` to the workgroup ARN `arn:aws:athena:{region}:{account}:workgroup/claws-readonly`; Lambda can no longer run queries in any workgroup
- **OpenSearch error messages exposing cluster internals (#87):** `execute_opensearch()` now returns generic messages for source_id parse failures, JSON decode errors, and search exceptions; raw endpoint URLs, index names, and opensearchpy exception details are logged at DEBUG level only and never returned to callers

## [0.11.0] - 2026-04-03

### Added
- **IRB approval workflow:** `plan` accepts `requires_irb: true` flag; sets `status: pending_approval` instead of `ready`; `excavate` blocks any plan with non-`ready` status with an actionable `pending_approval` message including the `plan_id`
- **`approve_plan` internal Lambda:** Receives `plan_id` and `approver_principal`; validates approver is in `CLAWS_IRB_APPROVERS` env var allowlist; blocks self-approval; sets `status: ready` in DynamoDB; emits `claws.irb / PlanApproved` EventBridge event
- **`plan.approve` Cedar action:** `irb_approver` role has explicit `permit(Action::"plan.approve")` in `policies/templates/phi-approved.cedar`; non-approvers cannot fire approve_plan
- **FERPA Guardrail preset:** `guardrails/ferpa/ferpa_guardrail.json`; denied topics: `student-pii-export`, `ferpa-evasion`, `grade-disclosure`, `directory-waiver-bypass`, `education-records-bulk`; regex patterns: SSN (`\b\d{3}-\d{2}-\d{4}\b`) and student ID (`[Ss][Tt][Uu][Dd]-\d{6,9}`) with BLOCK action; deploy with CDK context `enable_ferpa_guardrail: true`
- **Four Cedar policy templates** in `policies/templates/`: `read-only.cedar` (no excavate/export, metadata only), `no-pii-export.cedar` (forbids export when data_classification includes pii), `approved-domains-only.cedar` (locks principal to a pre-approved domain list), `phi-approved.cedar` (PHI data: clearance >= 3, IRB approval required, 3-row probe limit, HITL token required for excavate)
- **`audit_export` internal Lambda:** Scans CloudWatch Logs for audit records in a date range; writes one NDJSON file per day per tool to `s3://claws-runs-{account}/audit-exports/`; fields include `principal`, `tool`, `inputs_hash` (SHA-256), `outputs_hash` (SHA-256), `cost_usd`, `guardrail_trace`, `timestamp` — no raw PII in the output
- 25 new tests: IRB plan status, approve_plan handler (allowlist, self-approval block, EventBridge emit), FERPA guardrail config validation, Cedar template policy checks

## [0.10.0] - 2026-04-03

### Added
- **Source registry integration:** `discover` domain `"registry"` queries the `qs-data-source-registry` DynamoDB table (from quick-suite-data v0.6.0); sources registered via `register-source` Lambda in the Data stack appear as discoverable clAWS sources; SSM parameter `/quick-suite/data/source-registry-arn` is read by CDK to wire the IAM grant
- **Catalog-aware `discover`:** When `scope.domains` includes `"registry"`, `discover` returns sources from the cross-stack registry alongside Glue and S3 sources; `source_id` format for registry sources: `registry:{source_type}:{source_name}`
- **`audit_export` Lambda (initial):** Included in `ClawsStorageStack`; CloudWatch Logs source, NDJSON output to S3 (full SHA-256-hashed field set added in v0.11.0)
- 8 new tests for registry discover path and cross-stack source resolution

## [0.9.0] - 2026-04-03

### Added
- **`team_plans` tool Lambda:** Lists all plans for a given `team_id`; returns id, status, objective summary, created_by, created_at, shared_with; read-only; Cedar action `claws.team_plans`
- **`share_plan` tool Lambda:** Plan owner adds or removes principals from `shared_with` list; non-owners receive 403; Cedar action `claws.share_plan`; `shared_with` list stored on plan DynamoDB item
- `excavate` shared-plan support: principals in `shared_with` can execute a plan they do not own; ownership check relaxed to `principal == owner OR principal in shared_with`
- `team_plans` and `share_plan` registered as AgentCore Gateway Lambda targets in `ClawsToolsStack`
- 12 new tests: team_plans listing, share_plan grant/revoke, excavate shared-plan access, non-owner 403

## [0.8.0] - 2026-04-03

### Added
- **Plan ownership fields:** `plan` stores `team_id` (from request), `created_by` (from principal), `status` (always `ready` in v0.8.0; `pending_approval` added in v0.11.0) on the DynamoDB plan item
- **PII scan on probe samples:** `probe` runs `ApplyGuardrail` on all sample rows before returning; returns `status: blocked` (no samples returned) if any sample triggers the PII/PHI Guardrail config; previously only the base config was applied, not the data-specific scan
- **`excavate` ownership enforcement:** `excavate` loads the plan from DynamoDB and checks `principal == plan.created_by`; returns 403 if the caller is not the plan owner (or in `shared_with`, added v0.9.0)
- **`merge` refine operation:** `{"op": "merge", "source_run_id": "run-abc"}` merges two result sets on a configurable key; deduplicates on merge key; used by feed watches to accumulate results across scheduled runs
- **Feed watch type:** `watch` accepts `watch_type: "feed"` which appends new excavation results to a running S3 file rather than overwriting; uses `merge` refine operation internally
- **Export append/overwrite mode:** `export` accepts `mode: "append"` for S3 destinations; appends NDJSON lines to the destination key rather than replacing the file; provenance file updated with merged row count
- 15 new tests: plan ownership enforcement, PII probe scan, merge operation, feed watch behavior, export append mode

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

[Unreleased]: https://github.com/scttfrdmn/quick-suite-claws/compare/v0.11.0...HEAD
[0.11.0]: https://github.com/scttfrdmn/quick-suite-claws/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/scttfrdmn/quick-suite-claws/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/scttfrdmn/quick-suite-claws/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/scttfrdmn/quick-suite-claws/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/scttfrdmn/quick-suite-claws/compare/v0.6.1...v0.7.0
[0.6.1]: https://github.com/scttfrdmn/quick-suite-claws/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/scttfrdmn/quick-suite-claws/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/scttfrdmn/quick-suite-claws/compare/v0.4.1...v0.5.0
[0.4.1]: https://github.com/scttfrdmn/quick-suite-claws/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/scttfrdmn/quick-suite-claws/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/scttfrdmn/quick-suite-claws/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/scttfrdmn/quick-suite-claws/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/scttfrdmn/quick-suite-claws/releases/tag/v0.1.0
