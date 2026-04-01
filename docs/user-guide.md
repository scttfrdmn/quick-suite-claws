# clAWS User Guide

Reference documentation for operators deploying clAWS and developers integrating it.

For a step-by-step first deployment, see [getting-started.md](getting-started.md).
For component diagrams and CDK stack internals, see [architecture.md](architecture.md).
For the Cedar + Guardrails threat model, see [safety-model.md](safety-model.md).

---

## Tool pipeline overview

The six tools form a linear pipeline. An agent always calls them in order; each tool's
output feeds the next.

```
discover → probe → plan → excavate → refine → export
```

- `discover` and `probe` are read-only metadata operations (no query execution).
- `plan` is the only tool with free-text input. It returns a concrete query — the agent
  reviews it, then passes it verbatim to `excavate`.
- `excavate` runs exactly what the plan said. Any modification to the query returns a 403.
- `refine` and `export` operate on the stored result, not the raw data.

For the full pipeline table with policy gates, storage systems, and CDK details, see
[architecture.md](architecture.md).

---

## Each tool in depth

### `discover` — find data sources

**Purpose:** Search the data catalog for sources that match a query, within approved domains
and spaces.

**Inputs:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `query` | string | yes | Natural language search terms (e.g., "BRCA1 variant annotations") |
| `scope.domains` | string[] | yes | Data source types to search: `"athena"`, `"opensearch"`, `"s3"` |
| `scope.spaces` | string[] | no | Named spaces to restrict search (e.g., `"genomics-shared"`) |
| `limit` | integer | no | Max sources to return. Default: 10. Max: 100 |

**Outputs:**

| Field | Type | Description |
|-------|------|-------------|
| `sources[].id` | string | Source ID used in all subsequent tool calls |
| `sources[].kind` | string | `"table"`, `"index"`, or `"object"` |
| `sources[].confidence` | float | 0.0–1.0 relevance score based on name/tag match |
| `sources[].reason` | string | Human-readable explanation of the confidence score |

**What can go wrong:**

| Error | Cause |
|-------|-------|
| `400 scope.domains is required` | Missing required field |
| Empty `sources` array | No sources found in the given spaces/domains. Verify the Glue table, OpenSearch domain, or S3 bucket has the correct `claws:space` tag |
| Cedar `403` | Principal's `approved_domains` or `approved_spaces` doesn't include the requested scope |

**Safety:** No Guardrails scan on discover. Source metadata only — no data content is
returned.

---

### `probe` — inspect schema and samples

**Purpose:** Retrieve the schema, sample rows, size estimates, and cost estimates for a
specific source.

**Inputs:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source_id` | string | yes | Source ID from `discover` (e.g., `"athena:oncology.variant_index"`) |
| `mode` | string | yes | `"schema_only"`, `"schema_and_samples"`, `"cost_estimate"`, or `"full"` |
| `sample_rows` | integer | no | Number of sample rows to return. Default: 5. Max: 100 |

**Outputs:**

| Field | Type | Description |
|-------|------|-------------|
| `schema` | object | Column names and types; for Athena includes partition keys |
| `samples` | object[] | Sample rows (guardrail-scanned before return) |
| `row_count_estimate` | integer | Approximate row count |
| `size_bytes_estimate` | integer | Approximate uncompressed size in bytes |
| `cost_estimates` | object | Scan cost estimates; for Athena includes partition pruning estimates |

**What can go wrong:**

| Error | Cause |
|-------|-------|
| `status: blocked` | Sample rows triggered a guardrail (PII/PHI found). The samples are not returned. Review the data for sensitive content before probing. |
| `400 source not found` | Source ID doesn't exist or clAWS can't reach it. Verify source tagging and IAM permissions. |
| Cedar `403` | Source not in principal's `approved_sources`, or `sample_rows` exceeds `max_sample_rows` |

**Safety:** `ApplyGuardrail` scans all sample rows before returning them. If any sample
contains data matching the base PII/PHI config (SSN, MRN, card numbers) or a tenant overlay,
the entire probe call returns `status: blocked`.

The probe result schema is cached in DynamoDB (`claws-schemas` table, 1-hour TTL). The `plan`
tool reads this cache; run `probe` before `plan` on the same source.

---

### `plan` — generate a concrete query

**Purpose:** Translate a free-text objective into a concrete, reviewable execution plan with
the exact query, cost estimate, and output schema. This is the only tool that invokes an LLM.

**Inputs:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `objective` | string | yes | Free-text description of what you want (e.g., "Count pathogenic BRCA1 variants by cohort") |
| `source_id` | string | yes | Source to query. Must have been probed recently (schema cache required) |
| `constraints.max_cost_dollars` | float | no | Maximum acceptable query cost. Cedar enforces this against `principal.max_cost_per_excavation` |
| `constraints.max_bytes_scanned` | integer | no | Maximum bytes to scan (Athena only) |
| `constraints.timeout_seconds` | integer | no | Query timeout |
| `constraints.read_only` | boolean | yes | Must be `true`. Cedar requires this on all plan + excavate calls |

**Outputs:**

| Field | Type | Description |
|-------|------|-------------|
| `plan_id` | string | Unique plan identifier (`plan-{8 hex chars}`). Pass to `excavate`. |
| `status` | string | `"ready"` / `"rejected"` / `"blocked"` |
| `reason` | string | Explanation when status is not `"ready"` |
| `steps[0].input` | object | The exact `{source_id, query, query_type}` to pass to `excavate` |
| `estimated_cost` | string | Estimated cost (e.g., `"$0.24"`) |
| `estimated_bytes_scanned` | integer | Athena scan estimate in bytes |
| `output_schema` | object[] | Expected output columns with types |

**Status values:**

| Status | Meaning |
|--------|---------|
| `ready` | Plan passed all validation. Proceed to `excavate`. |
| `rejected` | SQL validator blocked a mutation (`UPDATE`, `DELETE`, `DROP`, `INSERT`) or a multi-statement attack (`;` separator). The query would have modified data. |
| `blocked` | Bedrock Guardrails blocked the objective. Either injection was detected in the objective text, or the objective matches a denied topic. |

**What can go wrong:**

| Error | Cause |
|-------|-------|
| `422 No schema found for source` | `probe` hasn't been run on this source, or the schema cache expired (1-hour TTL). Run `probe` first. |
| Cedar `403` | `read_only != true`, `max_cost_dollars` exceeds principal's limit, or source not approved |

---

### `excavate` — execute the query

**Purpose:** Run the exact query from the plan. Takes deterministic inputs — no LLM, no
free text.

**Inputs:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `plan_id` | string | recommended | ID of the plan this query comes from. Enables bait-and-switch protection. |
| `source_id` | string | yes | Must match the plan's `source_id` |
| `query` | string | yes | The exact query string from `steps[0].input.query` |
| `query_type` | string | yes | `"athena_sql"`, `"opensearch_dsl"`, or `"s3_select_sql"` |
| `constraints` | object | no | `max_bytes_scanned`, `timeout_seconds`, `read_only` |

**Outputs:**

| Field | Type | Description |
|-------|------|-------------|
| `run_id` | string | Unique result identifier (`run-{8 hex chars}`). Pass to `refine` and `export`. |
| `status` | string | `"complete"` / `"timeout"` / `"error"` / `"blocked"` |
| `rows_returned` | integer | Number of rows in the result |
| `bytes_scanned` | integer | Bytes scanned (Athena; 0 for OpenSearch and S3 Select) |
| `cost` | string | Formatted cost string (e.g., `"$0.22"`) |
| `result_uri` | string | S3 URI where the full result is stored |
| `result_preview` | object[] | First 5 rows of the result (excluded from audit log) |

**Bait-and-switch protection:**

When `plan_id` is provided, `excavate` loads the stored plan from DynamoDB and performs a
string equality check against the submitted `query`. If they differ by even one character,
the call returns:

```json
HTTP 403
{"error": "Query does not match stored plan. Submit the exact query from the plan."}
```

This prevents a scenario where an agent (or a prompt injection attack) modifies the query
between Cedar approval and execution. Always pass `plan_id` and copy `query` from the plan
response verbatim.

**What can go wrong:**

| Error | Cause |
|-------|-------|
| `403 Query does not match stored plan` | Query was modified. Copy `steps[0].input.query` exactly from the plan response. |
| `404 Plan not found` | Plan expired (24-hour DynamoDB TTL). Re-run `plan` to get a fresh `plan_id`. |
| `status: blocked` | Results triggered a guardrail scan. Raw results are stored in S3 for audit but not returned to the agent. |
| `status: timeout` | Query exceeded `timeout_seconds`. Increase the timeout or add partition filters to reduce scan scope. |

---

### `refine` — process results

**Purpose:** Apply post-processing operations to a stored result set: deduplicate, rank,
filter, normalize, and summarize.

**Inputs:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `run_id` | string | yes | Result ID from `excavate` |
| `operations` | array | yes | Ordered list of operations to apply (see below) |
| `top_k` | integer | no | Max rows to return after all operations. Default: 25. Max: 1000 |
| `output_format` | string | no | `"json"` (default), `"csv"`, or `"parquet"` |

**Operations:**

| Operation | Type | Description |
|-----------|------|-------------|
| `"dedupe"` | string | Remove exact duplicate rows |
| `"rank"` | string | Sort rows by numeric fields descending |
| `"normalize"` | string | Lowercase strings, strip whitespace |
| `"summarize"` | string | Generate an LLM summary of the result set (uses Bedrock with grounding check) |
| `{"op": "filter", "field": "...", "operator": "...", "value": ...}` | dict | Filter rows by field value |

Filter operators: `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `contains`, `not_contains`.
Example: `{"op": "filter", "field": "score", "operator": "gte", "value": 0.7}`.

Mixed-type lists are supported: `["dedupe", {"op": "filter", "field": "n", "operator": "gt", "value": 10}, "summarize"]`.

**What can go wrong:**

| Error | Cause |
|-------|-------|
| `404 run_id not found` | Result not in S3. The `run_id` from excavate may be from a different deployment or was cleaned up. |
| Cedar `403` | `resource.owner != principal` or `top_k > max_refine_top_k` |

---

### `export` — materialize results

**Purpose:** Write the final result to a destination with an optional provenance chain.
Final `ApplyGuardrail` scan runs before any write.

**Inputs:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `run_id` | string | yes | Result ID from `excavate` (or the `run_id` returned by `refine`) |
| `destination.type` | string | yes | `"s3"`, `"eventbridge"`, or `"callback"` |
| `destination.uri` | string | yes | Destination URI (see format per type below) |
| `include_provenance` | boolean | no | Write a provenance file alongside results. Default: `true` |

**Destination formats:**

| Type | URI format | Example |
|------|-----------|---------|
| `s3` | `s3://bucket/path/to/file.json` | `s3://research-outputs/run-abc/results.json` |
| `eventbridge` | `events://bus-name/detail-type` | `events://claws-bus/ClawsExportReady` |
| `callback` | Any HTTPS URL | `https://api.example.com/webhooks/claws` |

**S3 export:** Writes the result as JSON. If `include_provenance` is true, writes a
second file at `{key-without-extension}.provenance.json`.

**EventBridge export:** Publishes a single event to the specified bus with:
```json
{
  "Source": "claws",
  "DetailType": "<detail-type from URI>",
  "Detail": "{\"export_id\": \"...\", \"row_count\": N, \"payload\": [...]}"
}
```
If `FailedEntryCount > 0` in the response, the call returns `500`.

**Callback export:** POSTs the result as JSON to the HTTPS URL with headers:
- `Content-Type: application/json`
- `X-Claws-Export-Id: {export_id}`
- `X-Claws-Signature: sha256={hmac_hex}` — only if `CLAWS_CALLBACK_SECRET` env var is set

To enable HMAC-SHA256 request signing on the callback Lambda, set the environment variable
`CLAWS_CALLBACK_SECRET` in `ClawsToolsStack`. The signature covers the full JSON body.
Verify it on the receiving end:

```python
import hashlib, hmac
expected = hmac.new(
    secret.encode(), body_bytes, hashlib.sha256
).hexdigest()
assert header == f"sha256={expected}"
```

**Provenance chain structure:**

```json
{
  "export_timestamp": "2026-04-01T18:30:00.000000+00:00",
  "principal": "research-agent-prod",
  "run_id": "run-x7y8z9ab",
  "destination": {"type": "s3", "uri": "s3://..."},
  "chain": {
    "note": "Full provenance chain: plan → query → raw result → refinement → export",
    "run_id": "run-x7y8z9ab"
  }
}
```

**What can go wrong:**

| Error | Cause |
|-------|-------|
| Cedar `403` | Destination URI not in principal's `approved_export_targets` |
| `status: blocked` | Final payload triggered a guardrail scan. No write occurred. |
| `500 EventBridge entries failed` | EventBridge `put_events` returned `FailedEntryCount > 0`. Check bus name and IAM permissions. |
| `500 Callback export failed` | The HTTPS endpoint returned a non-2xx status or was unreachable. |

---

## Cedar policy authoring guide

Cedar policies are evaluated at the AgentCore Gateway before any Lambda executes.

### Principal entity model

Every principal has these attributes, which Cedar policies reference in `when` clauses:

| Attribute | Type | Used in |
|-----------|------|---------|
| `approved_spaces` | string[] | `discover` scope check |
| `approved_domains` | string[] | `discover` domain check |
| `approved_sources` | string[] | `probe`, `plan`, `excavate` source checks |
| `max_sample_rows` | integer | `probe` sample cap |
| `max_cost_per_excavation` | float | `plan` cost constraint cap |
| `byte_scan_limit` | integer | `excavate` scan cap |
| `max_refine_top_k` | integer | `refine` top_k cap |
| `approved_export_targets` | string[] | `export` destination allowlist |
| `clearance_level` | integer | Used by the `restricted-dataset` example policy |

### Walking through `policies/default.cedar`

The default policy is deployed to all principals. Here is what each rule does:

**discover** — allows scoped to approved spaces and domains:
```cedar
permit(principal, action == Action::"discover", resource)
when {
  context.scope.spaces.containsAll(principal.approved_spaces) &&
  context.scope.domains.containsAll(principal.approved_domains)
};
```
The `containsAll` check means the requested spaces must be a subset of the principal's
approved spaces. If an agent tries to discover in `["patient-data"]` and the principal only
has `["genomics-shared"]`, the permit won't fire.

**excavate** — the strictest gate:
```cedar
permit(principal, action == Action::"excavate", resource)
when {
  context.read_only == true &&
  context.max_bytes_scanned <= principal.byte_scan_limit &&
  resource in principal.approved_sources &&
  context.plan_id != ""
};
```
The `context.plan_id != ""` check requires that a non-empty `plan_id` was submitted.
This ensures excavate is always plan-linked — ad-hoc queries without a plan_id are rejected
at the Cedar layer before any Lambda code runs.

**export** — destination allowlist with explicit deny:
```cedar
permit(principal, action == Action::"export", resource)
when {
  context.destination.uri in principal.approved_export_targets &&
  resource.owner == principal
};

forbid(principal, action == Action::"export", resource)
unless {
  context.destination.uri in principal.approved_export_targets
};
```
The `forbid` clause is critical. Without it, Cedar's default is "deny unless permitted,"
but adding the explicit `forbid ... unless` makes the intent unambiguous and prevents
policy-stacking errors where a more permissive policy could inadvertently allow export
to an unapproved destination.

### `policies/examples/research-team.cedar` — group-based policy

```cedar
permit(
  principal in Group::"research-team",
  action == Action::"discover",
  resource
)
when {
  context.scope.spaces.containsAny(
    ["research-public", "genomics-shared", "proteomics-shared"]
  )
};
```

Key differences from the default policy:
- `principal in Group::"research-team"` scopes this policy to a group, not all principals.
- `containsAny` instead of `containsAll` — the request needs to include *at least one* of
  the approved spaces, not all of them.
- Source wildcards in `probe`: `Source::"athena:genomics.*"` matches any Athena source in
  the `genomics` database.
- Export limited to two specific prefixes: `s3://research-outputs/` and `s3://team-shared/`.

### `policies/examples/restricted-dataset.cedar` — clearance + HITL

For PHI, HIPAA, and other restricted datasets. Key additions:

- Every action requires `principal.clearance_level >= 2`.
- Probe is limited to 3 sample rows (default is 5+).
- Excavate requires `context.hitl_approval_id != ""` — a human review token.
- Export requires `hitl_approval_id`, `include_provenance == true`, and only allows
  destinations in `principal.restricted_export_targets` (a separate, tighter allowlist).
- A blanket `forbid` for `clearance_level < 2` ensures the deny fires regardless of other
  permits in the stack:

```cedar
forbid(principal, action, resource in ResourceGroup::"restricted")
when { principal.clearance_level < 2 };
```

### Policy template for a new team

```cedar
// Cedar policy for team: <YOUR_TEAM_NAME>
// Deploy alongside default.cedar — both apply to all matching principals

permit(
  principal in Group::"<your-team>",
  action == Action::"discover",
  resource
)
when {
  // Spaces this team can discover in
  context.scope.spaces.containsAny(["<space-1>", "<space-2>"])
};

permit(
  principal in Group::"<your-team>",
  action == Action::"probe",
  resource
)
when {
  resource in [Source::"athena:<your-database>.*"] &&
  context.sample_rows <= 10  // Adjust based on data sensitivity
};

permit(
  principal in Group::"<your-team>",
  action == Action::"plan",
  resource
)
when {
  context.constraints.read_only == true &&
  context.constraints.max_cost_dollars <= 2.00  // Per-query cost cap in USD
};

permit(
  principal in Group::"<your-team>",
  action == Action::"excavate",
  resource
)
when {
  context.read_only == true &&
  context.max_bytes_scanned <= 2000000000 &&  // 2 GB scan limit
  context.plan_id != "" &&
  resource in principal.approved_sources
};

permit(
  principal in Group::"<your-team>",
  action == Action::"export",
  resource
)
when {
  context.destination.uri.startsWith("s3://<your-output-bucket>/")
};

// Explicit deny for any export not in the allowlist
forbid(
  principal in Group::"<your-team>",
  action == Action::"export",
  resource
)
unless {
  context.destination.uri.startsWith("s3://<your-output-bucket>/")
};
```

### Common mistakes

**Forgetting the `forbid` clause on export.** Without it, a principal with no matching
`permit` just gets a Cedar deny (correct behavior), but stacking a second more permissive
policy can inadvertently add a permit. The `forbid ... unless` is the belt to the
permit's suspenders.

**`containsAll` vs `containsAny` for spaces.** `containsAll(approved)` means "every space
I'm requesting must be in the approved list." `containsAny(allowed)` means "at least one
space I'm requesting must be in the allowed list." For discovery, `containsAny` is usually
correct. For excavate source checks, `in principal.approved_sources` (a set membership
check) is clearer than either.

**Source wildcards are not substring matches.** `Source::"athena:genomics.*"` is a Cedar
entity hierarchy check, not a regex. The `.*` notation works because Cedar evaluates it
as an entity in the `athena:genomics` namespace. This may not work as expected with all
Cedar evaluation engines — test your policy with `cedar authorize` before deploying.

---

## Guardrail customization

### Three-layer structure

```
guardrails/base/
├── content-filters.json     # Sexual, violence, hate, misconduct, injection
├── injection-detection.json # PROMPT_ATTACK configuration (informational)
└── pii-entities.json        # PII/PHI entity types and actions

guardrails/tenants/
├── genomics-research.json   # Extends base: denied topics for genomics
└── financial-data.json      # Extends base: denied topics for financial data
```

The base configs apply to all deployments. Tenant overlays are additive — they extend
the base with domain-specific topics, patterns, and word filters.

### Base PII/PHI entities (`guardrails/base/pii-entities.json`)

Two action types determine what happens when a match is detected:

**BLOCK** — the entire API call returns `status: blocked`. No data is returned.
Applied to high-risk identifiers:

| Entity type | Example |
|-------------|---------|
| `US_SOCIAL_SECURITY_NUMBER` | 123-45-6789 |
| `CREDIT_DEBIT_CARD_NUMBER` | 4111-1111-1111-1111 |
| `CREDIT_DEBIT_CARD_CVV` | 123 |
| `PIN` | (payment PIN patterns) |
| `PASSWORD` | (password-like strings) |
| `AWS_ACCESS_KEY` | AKIA... |
| `AWS_SECRET_KEY` | (40-char secrets) |
| `US_INDIVIDUAL_TAX_IDENTIFICATION_NUMBER` | 12-3456789 |

**ANONYMIZE** — the matched value is replaced with a placeholder, but the call continues
with the redacted data. Applied to lower-risk identifiers:

| Entity type | Replacement |
|-------------|-------------|
| `EMAIL` | `{EMAIL}` |
| `PHONE` | `{PHONE}` |
| `NAME` | `{NAME}` |
| `ADDRESS` | `{ADDRESS}` |
| `AGE` | `{AGE}` |
| `DATE_TIME` | `{DATE_TIME}` |
| `IP_ADDRESS` | `{IP_ADDRESS}` |
| `URL` | `{URL}` |

### Custom regex patterns

Add patterns to `pii-entities.json` under `"regexes"`. Each entry:

```json
{
  "name": "my_pattern",
  "description": "Human-readable description",
  "pattern": "REGEX-PATTERN",
  "action": "BLOCK"
}
```

The three existing patterns:

| Name | Pattern | Action | Matches |
|------|---------|--------|---------|
| `internal_project_code` | `PRJ-\d{6}` | ANONYMIZE | Internal project IDs like `PRJ-004712` |
| `medical_record_number` | `MRN[-:]?\s?\d{6,10}` | BLOCK | Medical record numbers |
| `aws_account_id` | `\b\d{12}\b` | ANONYMIZE | 12-digit AWS account IDs |

To add your own pattern, append to the `regexes` array and redeploy `ClawsGuardrailsStack`.

### Content filters (`guardrails/base/content-filters.json`)

| Filter | Input strength | Output strength | Notes |
|--------|---------------|-----------------|-------|
| `SEXUAL` | HIGH | MEDIUM | |
| `VIOLENCE` | HIGH | MEDIUM | |
| `HATE` | HIGH | MEDIUM | |
| `INSULTS` | HIGH | MEDIUM | |
| `MISCONDUCT` | HIGH | MEDIUM | |
| `PROMPT_ATTACK` | HIGH | **NONE** | Intentional: injection detection fires on input only |

`PROMPT_ATTACK` is set to HIGH on input and NONE on output deliberately. The guardrail
evaluates the agent's objective when it arrives at the `plan` tool. It does not re-evaluate
the generated SQL/DSL on its way back out — that output is deterministic code, not a
content risk.

### Tenant overlays

Overlays extend the base config with domain-specific denied topics and additional PII
patterns. They are registered in `ClawsGuardrailsStack` and associated with the same
`CLAWS_GUARDRAIL_ID`.

**`guardrails/tenants/genomics-research.json`** adds:
- Denied topic: `participant-reidentification` — blocks attempts to correlate participant
  identifiers across studies.
- Denied topic: `germline-discrimination` — blocks outputs that could support genetic
  discrimination.
- Additional PII patterns: study participant IDs (`SUBJ-XXXX`), biobank sample IDs
  (`BB-XXXXXX`).

**`guardrails/tenants/financial-data.json`** adds:
- Denied topic: `insider-trading` — blocks material non-public information references.
- Denied topic: `market-manipulation` — blocks outputs related to price manipulation.
- Additional PII patterns: bank account numbers, ABA routing numbers, CUSIP codes.
- Word filters: `MNPI`, `MATERIAL-NONPUBLIC`, `PRE-RELEASE` (hard block on these terms).

### How to add a tenant overlay

1. Create `guardrails/tenants/your-tenant.json` following the existing format:

```json
{
  "description": "Overlay for <your domain>",
  "extends": "claws-base",
  "deniedTopics": [
    {
      "name": "your-topic",
      "description": "What this topic blocks",
      "examples": [
        "Example phrase that should trigger this topic",
        "Another example"
      ]
    }
  ],
  "additionalPiiPatterns": [
    {
      "name": "your_pattern",
      "description": "Description",
      "pattern": "YOUR-REGEX",
      "action": "BLOCK"
    }
  ]
}
```

2. Register the overlay in `infra/cdk/stacks/guardrails_stack.py`.

3. Redeploy `ClawsGuardrailsStack`:
   ```bash
   cdk deploy ClawsGuardrailsStack
   ```

4. The new Guardrail ID is printed as a CDK output. Update the `CLAWS_GUARDRAIL_ID`
   environment variable on the Lambda functions by redeploying `ClawsToolsStack`:
   ```bash
   cdk deploy ClawsToolsStack
   ```

---

## Audit trail

Every tool call writes a structured JSON record to CloudWatch Logs via `audit_log()` in
`tools/shared.py`. The record fields:

| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | string | ISO 8601 UTC timestamp |
| `tool` | string | Tool name: `"discover"`, `"probe"`, `"plan"`, `"excavate"`, `"refine"`, `"export"` |
| `principal` | string | Caller identity from the AgentCore Gateway authorizer |
| `request_id` | string | Lambda request ID for correlation with CloudWatch Logs |
| `inputs` | object | Full request body (minus secrets) |
| `outputs` | object | Response body with `result_preview` excluded |
| `cost` | float or null | Execution cost in USD (null for non-executing tools) |
| `guardrail_trace` | object or null | Guardrail assessment details when a scan ran |

`result_preview` is explicitly excluded from the audit log for size reasons. The full
result is at `result_uri` in S3.

To route audit logs to a Quick Suite dashboard for real-time monitoring, see
[docs/quick-suite-integration.md](quick-suite-integration.md).

---

## CloudWatch metrics

clAWS emits metrics to the `claws` namespace (controlled by the `CLAWS_METRICS_NAMESPACE`
environment variable). Metrics are skipped when the variable is unset — this is intentional
for local development and testing, where CloudWatch calls would fail or produce noise.

**Metrics emitted per tool call:**

| Metric | Unit | Description |
|--------|------|-------------|
| `Invocations` | Count | Every tool call |
| `Errors` | Count | Calls returning `status: error` |
| `GuardrailBlocks` | Count | Calls returning `status: blocked` |
| `Timeouts` | Count | Calls returning `status: timeout` |
| `CostDollars` | None | Cost in USD (excavate and export only) |
| `RowsReturned` | Count | Rows returned by successful excavate calls |

All metrics have a `Tool` dimension (e.g., `Tool=excavate`). This lets you build per-tool
dashboards and alarms.

**Example CloudWatch Insights query** for cost trending over the past 7 days:

```
fields @timestamp, tool, cost
| filter tool = "excavate" and cost != null
| stats sum(cost) as total_cost_usd by bin(1d)
| sort @timestamp desc
```

**Example alarm** for guardrail block rate:

```python
# In ClawsToolsStack or a monitoring stack
cloudwatch.Alarm(
    self, "GuardrailBlockAlarm",
    metric=cloudwatch.Metric(
        namespace="claws",
        metric_name="GuardrailBlocks",
        dimensions_map={"Tool": "excavate"},
        statistic="Sum",
        period=cdk.Duration.minutes(5),
    ),
    threshold=10,
    evaluation_periods=1,
    alarm_description="More than 10 guardrail blocks on excavate in 5 minutes",
)
```
