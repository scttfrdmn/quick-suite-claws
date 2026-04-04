# clAWS Compliance Guide

Deployment and operational guidance for regulated research and institutional data
environments. Covers FERPA-protected student records, IRB-governed research data, and
general HIPAA/PHI considerations.

---

## Overview: Two independent safety layers

clAWS enforces two separate safety layers on every tool call. Neither layer alone is
sufficient for regulated data. Both must be deployed together.

| Layer | Technology | What it catches |
|-------|-----------|----------------|
| **Structural (Cedar)** | Cedar policy engine at AgentCore Gateway | Permission violations, principal scope overreach, query cost limits, export destination restrictions |
| **Semantic (Bedrock Guardrails)** | ML-based content safety via `ApplyGuardrail` API | PII in probe samples, PHI in excavation results, prompt injection in plan objectives, FERPA-specific topic blocks |

Cedar fires before any Lambda runs — a denied Cedar decision means zero bytes are scanned
and no LLM is invoked. Guardrails fire at LLM I/O and directly on data (probe samples,
excavation results, export payloads) without involving the LLM.

An attacker who wanted to extract restricted data would need to simultaneously bypass a
deterministic policy engine and a content safety model — two completely different attack
surfaces.

---

## IRB Workflow

When research data is governed by an Institutional Review Board protocol, plans can require
explicit reviewer approval before any query executes.

### How it works

```
plan(requires_irb: true)
         │
         ▼
   status: pending_approval
   (excavate returns 403 until approved)
         │
         ▼
approve_plan(plan_id, approver_principal)
         │  validates: approver in CLAWS_IRB_APPROVERS
         │  blocks: approver == plan.created_by (no self-approval)
         ▼
   status: ready
   excavate unblocked
         │
         ▼
   EventBridge: claws.irb / PlanApproved
   (for audit trail and Quick Automate triggers)
```

### Configuration

1. **Set the approver allowlist** — add `CLAWS_IRB_APPROVERS` to the `approve_plan` Lambda
   environment variable as a comma-separated list of principal IDs:

   ```bash
   cdk deploy --context irb_approvers="irb-reviewer-001,irb-director-002"
   ```

2. **Restrict `approve_plan` invocation** — the Lambda should only be invocable by
   authorized IRB staff. Grant invoke permission to a specific IAM role or user:

   ```bash
   aws lambda add-permission \
     --function-name qs-claws-approve-plan \
     --statement-id AllowIRBRole \
     --action lambda:InvokeFunction \
     --principal arn:aws:iam::ACCOUNT:role/irb-reviewer-role
   ```

3. **Cedar policy for IRB approval action** — use `policies/templates/phi-approved.cedar`
   as a starting point. It includes:

   ```cedar
   permit(
     principal in Group::"irb_approver",
     action == Action::"plan.approve",
     resource
   );
   ```

4. **EventBridge rule for audit** — subscribe to `claws.irb / PlanApproved` events for
   downstream audit logging or Quick Automate notifications to the PI:

   ```bash
   aws events put-rule \
     --name claws-irb-approvals \
     --event-pattern '{"source": ["claws.irb"], "detail-type": ["PlanApproved"]}' \
     --state ENABLED
   ```

### What gets logged

Every `approve_plan` invocation writes to CloudWatch Logs with:
- `plan_id`
- `approver_principal`
- `owner_principal` (the plan creator, verified not to be the approver)
- `approved_at` (ISO 8601 UTC)
- `irb_event_id` (EventBridge event ID for correlation)

---

## FERPA Configuration

The FERPA Guardrail preset blocks topics and patterns specific to the Family Educational
Rights and Privacy Act. It extends the base clAWS Guardrail config.

### What the FERPA preset blocks

**Denied topics (ML-based semantic detection):**

| Topic | What it catches |
|-------|----------------|
| `student-pii-export` | Attempts to extract student identifiers in bulk |
| `ferpa-evasion` | Prompts that try to work around FERPA restrictions |
| `grade-disclosure` | Grade-level disclosure to unauthorized principals |
| `directory-waiver-bypass` | Attempts to expose directory information for students who have filed a waiver |
| `education-records-bulk` | Bulk extraction of education records without aggregation |

**PII regex patterns (deterministic BLOCK):**

| Pattern | Regex | Action |
|---------|-------|--------|
| SSN | `\b\d{3}-\d{2}-\d{4}\b` | BLOCK |
| Student ID | `[Ss][Tt][Uu][Dd]-\d{6,9}` | BLOCK |

### Enabling FERPA

```bash
cdk deploy --context enable_ferpa_guardrail=true
```

This deploys `guardrails/ferpa/ferpa_guardrail.json` as an additional Guardrail and
sets `CLAWS_FERPA_GUARDRAIL_ID` on all tool Lambdas. The FERPA Guardrail is applied
**in addition to** the base Guardrail — both run on every applicable tool call.

### Testing the FERPA preset

After deploying, verify the preset fires as expected:

```bash
# This should return status: blocked
aws lambda invoke \
  --function-name qs-claws-plan \
  --payload '{
    "objective": "Export all student IDs and grades for the 2024 cohort to a CSV file",
    "source_id": "athena:enrollment.student_grades",
    "constraints": {"read_only": true}
  }' \
  --cli-binary-format raw-in-base64-out /tmp/out.json && cat /tmp/out.json
```

---

## Cedar Policy Templates

Four pre-built templates in `policies/templates/`:

### `read-only.cedar`

**Use case:** Principals that should be able to discover and probe data sources, but never
query or export data. Suitable for metadata auditors or initial exploratory access.

```cedar
// Permits: discover, probe only
// Forbids: plan, excavate, refine, export explicitly
```

### `no-pii-export.cedar`

**Use case:** Principals who can run queries but cannot export results when the data
source is classified as containing PII. Suitable for analyst roles where aggregated
query results are fine but row-level export to S3 is not permitted for PII data.

Key clause:
```cedar
forbid(principal, action == Action::"export", resource)
when { context.data_classification.contains("pii") };
```

### `approved-domains-only.cedar`

**Use case:** Principals locked to a specific set of Glue databases or data domains.
Prevents lateral movement to data stores outside the principal's approved scope.

Replace `["your-space"]` with the appropriate space names for the team.

### `phi-approved.cedar`

**Use case:** Principals authorized to access PHI data under HIPAA or IRB protocol.
The strictest template. Requires:
- `clearance_level >= 3`
- `requires_irb: true` on all plans (combined with IRB workflow)
- Human-in-the-loop approval token (`hitl_approval_id`) on excavate
- Probe limited to 3 sample rows
- Export only to approved PHI destinations (not general-purpose S3 paths)

Deploy this template alongside `default.cedar` for IRB-governed cohorts.

### Deploying a template

Templates are `.cedar` files — deploy the same way as `default.cedar`:

```bash
# Reference the template in your CDK stack
aws cedar:create-policy \
  --policy-store-id PSxxxxxx \
  --definition file://policies/templates/read-only.cedar
```

Or via CDK `ClawsPolicyStack` — the stack reads all `.cedar` files in `policies/` and
registers them. Copy or symlink the template into `policies/` before deploying.

---

## Compliance Audit Export

The `audit_export` internal Lambda scans CloudWatch Logs for clAWS audit records and
writes compliance-ready NDJSON files to S3. No raw data or PII appears in the export —
inputs and outputs are SHA-256-hashed.

### Export fields

| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | string | ISO 8601 UTC |
| `tool` | string | `discover`, `probe`, `plan`, `excavate`, `refine`, `export` |
| `principal` | string | Caller identity |
| `inputs_hash` | string | SHA-256 of the full request body |
| `outputs_hash` | string | SHA-256 of the response body |
| `cost_usd` | float or null | Athena scan cost (null for non-executing tools) |
| `guardrail_trace` | object or null | Guardrail assessment summary when a scan ran |
| `request_id` | string | Lambda request ID for CloudWatch Logs correlation |

### Running an audit export

Invoke `audit_export` directly:

```bash
aws lambda invoke \
  --function-name qs-claws-audit-export \
  --payload '{
    "date_start": "2026-03-01",
    "date_end": "2026-03-31",
    "tools": ["excavate", "export"]
  }' \
  --cli-binary-format raw-in-base64-out /tmp/out.json && cat /tmp/out.json
```

Output is written to:
```
s3://claws-runs-{account}/audit-exports/{date_start}_{date_end}/{tool}.ndjson
```

### Using export output for compliance review

The hashed export can be provided to compliance auditors or used to verify that clAWS was
used appropriately during a given period. To verify a specific transaction:

1. Retrieve the original raw audit log from CloudWatch Logs using the `request_id`
2. Hash the inputs/outputs with SHA-256 and compare against the export hashes
3. Discrepancies indicate tampered audit records

This two-source approach (CloudWatch Logs + S3 hashed export) provides tamper evidence.

---

## Pre-Deployment Compliance Checklist

Use this checklist before deploying clAWS in a FERPA, HIPAA, or IRB-governed environment.

**Cedar policies:**
- [ ] `policies/default.cedar` restricts `approved_spaces` to only approved data spaces
- [ ] No wildcards in `approved_sources` for sensitive sources
- [ ] Explicit `forbid` clause on `export` for all restricted-data principals
- [ ] PHI principals use `phi-approved.cedar` template
- [ ] IRB-governed data requires `requires_irb: true` in plan (enforce via Cedar if needed)

**Guardrails:**
- [ ] FERPA preset enabled if any data is FERPA-governed (`enable_ferpa_guardrail: true`)
- [ ] Base PII Guardrail includes your institution's student ID regex pattern
- [ ] Guardrail tested against known-bad inputs before go-live

**IRB workflow:**
- [ ] `CLAWS_IRB_APPROVERS` populated with authorized principal IDs
- [ ] `approve_plan` Lambda invoke permissions locked to IRB role only
- [ ] Cedar policy includes `plan.approve` action for IRB group
- [ ] EventBridge rule configured for `claws.irb / PlanApproved` events

**Audit:**
- [ ] CloudWatch Logs retention set to ≥ 7 years for regulated data
- [ ] `audit_export` Lambda tested; S3 output bucket has appropriate access controls
- [ ] Compliance team has read access to audit export S3 prefix

**Networking:**
- [ ] If handling PHI: VPC mode enabled; tool Lambdas not internet-reachable
- [ ] S3 Gateway endpoint present in VPC if VPC mode is enabled
- [ ] No public S3 export destinations in Cedar policy `approved_export_targets`
