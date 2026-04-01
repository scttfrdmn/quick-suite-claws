# Example: Document Mining with S3 Select

End-to-end example of using clAWS to excavate structured data from documents stored in S3.
A compliance agent identifies indemnification clauses with uncapped liability across a
corpus of vendor contracts, ahead of a scheduled audit.

---

## Scenario

**Agent:** compliance review agent  
**Data source:** `s3://legal-corpus-prod/contracts/extracted/all-contracts-2024.parquet`  
**Goal:** Find all indemnification clauses where `liability_cap` is null (uncapped liability)

The contract corpus has been preprocessed into a single Parquet file with one row per
extracted clause. Parquet works well here because it supports predicate pushdown — S3 Select
scans only the columns and rows that match the filter, not the full file.

Document schema:

| Field | Type | Description |
|-------|------|-------------|
| `doc_id` | string | Internal contract identifier (e.g., `VND-2024-0047`) |
| `contract_type` | string | Contract category (`SaaS`, `MSA`, `NDA`, `SOW`, ...) |
| `counterparty` | string | Vendor name (may be anonymized by guardrail) |
| `section` | string | Section reference in the original document (e.g., `8.2`) |
| `clause_text` | string | Extracted clause text |
| `clause_type` | string | Clause category (`indemnification`, `limitation_of_liability`, ...) |
| `liability_cap` | string | Cap amount or `null` for uncapped clauses |
| `effective_date` | string | Contract effective date |

---

## Pipeline

### Step 1 — Discover

```json
POST /discover
{
  "query": "vendor contract indemnification clauses",
  "scope": {
    "domains": ["s3"],
    "spaces": ["legal-docs"]
  },
  "limit": 10
}
```

```json
{
  "sources": [
    {
      "id": "s3://legal-corpus-prod/contracts/extracted/all-contracts-2024.parquet",
      "kind": "object",
      "confidence": 0.84,
      "reason": "Prefix 'contracts/extracted/' matches query terms: contract"
    }
  ]
}
```

Carry forward: `sources[0].id`.

---

### Step 2 — Probe

```json
POST /probe
{
  "source_id": "s3://legal-corpus-prod/contracts/extracted/all-contracts-2024.parquet",
  "mode": "schema_and_samples",
  "sample_rows": 3
}
```

```json
{
  "source_id": "s3://legal-corpus-prod/contracts/extracted/all-contracts-2024.parquet",
  "schema": {
    "columns": [
      {"name": "doc_id",         "type": "string"},
      {"name": "contract_type",  "type": "string"},
      {"name": "counterparty",   "type": "string"},
      {"name": "section",        "type": "string"},
      {"name": "clause_text",    "type": "string"},
      {"name": "clause_type",    "type": "string"},
      {"name": "liability_cap",  "type": "string"},
      {"name": "effective_date", "type": "string"}
    ]
  },
  "samples": [
    {
      "doc_id": "VND-2024-0047",
      "contract_type": "SaaS",
      "counterparty": "ANON-CORP-001",
      "section": "8.2",
      "clause_text": "Vendor shall indemnify, defend, and hold harmless Customer from any claims arising from Vendor's gross negligence or willful misconduct.",
      "clause_type": "indemnification",
      "liability_cap": null,
      "effective_date": "2024-01-15"
    },
    {
      "doc_id": "VND-2024-0112",
      "contract_type": "MSA",
      "counterparty": "ANON-CORP-002",
      "section": "12.1",
      "clause_text": "Customer's total liability under this Agreement shall not exceed the fees paid in the preceding twelve months.",
      "clause_type": "limitation_of_liability",
      "liability_cap": "12_months_fees",
      "effective_date": "2024-03-01"
    },
    {
      "doc_id": "VND-2024-0203",
      "contract_type": "SaaS",
      "counterparty": "ANON-CORP-003",
      "section": "9.4",
      "clause_text": "Each party shall indemnify the other for any third-party claims, damages, and expenses.",
      "clause_type": "indemnification",
      "liability_cap": null,
      "effective_date": "2024-02-20"
    }
  ],
  "row_count_estimate": 3241,
  "size_bytes_estimate": 12582912
}
```

> **Safety:** The `counterparty` values in the sample rows above are shown as
> `ANON-CORP-001`, `ANON-CORP-002`, `ANON-CORP-003` — these are not the actual vendor
> names. Bedrock Guardrails detected the `counterparty` column values as entity type `NAME`
> and anonymized them (ANONYMIZE action) before returning the samples to the agent. The
> agent never sees the actual counterparty names, even in samples.

---

### Step 3 — Plan

```json
POST /plan
{
  "objective": "Find all indemnification clauses where liability_cap is null, indicating uncapped vendor liability",
  "source_id": "s3://legal-corpus-prod/contracts/extracted/all-contracts-2024.parquet",
  "constraints": {
    "max_cost_dollars": 0.05,
    "read_only": true
  }
}
```

```json
{
  "plan_id": "plan-d7e8f9a0",
  "status": "ready",
  "steps": [
    {
      "input": {
        "source_id": "s3://legal-corpus-prod/contracts/extracted/all-contracts-2024.parquet",
        "query": "SELECT s.doc_id, s.contract_type, s.section, s.clause_text, s.effective_date FROM S3Object s WHERE s.clause_type = 'indemnification' AND s.liability_cap IS NULL",
        "query_type": "s3_select_sql"
      },
      "description": "Select indemnification clauses with null liability cap from the contracts Parquet file"
    }
  ],
  "estimated_cost": "$0.00",
  "estimated_bytes_scanned": 9437184,
  "output_schema": [
    {"name": "doc_id",        "type": "string"},
    {"name": "contract_type", "type": "string"},
    {"name": "section",       "type": "string"},
    {"name": "clause_text",   "type": "string"},
    {"name": "effective_date","type": "string"}
  ]
}
```

> **Note on cost:** S3 Select pricing is $0.002 per GB of data scanned. 9.4 MB ×
> $0.002/GB ≈ $0.000017, which rounds to `"$0.0000"` at four decimal places. For larger
> files, partition your Parquet data by `clause_type` to reduce scan volume.

> **Injection detection:** Guardrails evaluated the `objective` before generating the S3
> Select SQL. An objective attempting to read all clause text across all contract types
> (circumventing the `WHERE clause_type = 'indemnification'` constraint) would have been
> evaluated for prompt injection patterns.

Carry forward: `plan_id` and `steps[0].input`.

---

### Step 4 — Excavate

```json
POST /excavate
{
  "plan_id": "plan-d7e8f9a0",
  "source_id": "s3://legal-corpus-prod/contracts/extracted/all-contracts-2024.parquet",
  "query": "SELECT s.doc_id, s.contract_type, s.section, s.clause_text, s.effective_date FROM S3Object s WHERE s.clause_type = 'indemnification' AND s.liability_cap IS NULL",
  "query_type": "s3_select_sql",
  "constraints": {
    "read_only": true
  }
}
```

```json
{
  "run_id": "run-c3d4e5f6",
  "status": "complete",
  "rows_returned": 23,
  "bytes_scanned": 9437184,
  "cost": "$0.0000",
  "result_uri": "s3://claws-runs/run-c3d4e5f6/result.json",
  "result_preview": [
    {
      "doc_id": "VND-2024-0047",
      "contract_type": "SaaS",
      "section": "8.2",
      "clause_text": "Vendor shall indemnify, defend, and hold harmless Customer from any claims arising from Vendor's gross negligence or willful misconduct.",
      "effective_date": "2024-01-15"
    },
    {
      "doc_id": "VND-2024-0203",
      "contract_type": "SaaS",
      "section": "9.4",
      "clause_text": "Each party shall indemnify the other for any third-party claims, damages, and expenses.",
      "effective_date": "2024-02-20"
    },
    {
      "doc_id": "VND-2024-0318",
      "contract_type": "MSA",
      "section": "11.1",
      "clause_text": "Vendor shall indemnify Customer against all losses, liabilities, costs, and expenses arising out of any breach of this Agreement.",
      "effective_date": "2024-04-08"
    }
  ]
}
```

> **Safety:** The 23 result rows were scanned by `ApplyGuardrail`. `clause_text` fields
> were checked for personal data — if any clause had mentioned an individual by name (e.g.,
> in a named-executive provision), that name would have been anonymized before the agent
> received the result. The agent sees sanitized clause extracts, not raw contract text.

Carry forward: `run_id`.

---

### Step 5 — Refine

```json
POST /refine
{
  "run_id": "run-c3d4e5f6",
  "operations": ["dedupe", "summarize"],
  "top_k": 25,
  "output_format": "json"
}
```

```json
{
  "run_id": "run-c3d4e5f6",
  "refined_uri": "s3://claws-runs/run-c3d4e5f6/refined.json",
  "manifest": {
    "operations_applied": ["dedupe", "summarize"],
    "rows_in": 23,
    "rows_out": 21,
    "dedupe": {"duplicates_removed": 2},
    "summarize": {
      "model": "amazon.nova-lite-v1:0",
      "grounding_check": "passed",
      "summary": "21 indemnification clauses with uncapped liability identified across 18 active contracts (15 SaaS, 3 MSA). The clauses follow three patterns: (1) mutual indemnification with no cap (8 contracts), (2) vendor-only uncapped indemnification for gross negligence or IP infringement (9 contracts), (3) broad uncapped indemnification for any breach (4 contracts, highest risk). Contracts VND-2024-0047, VND-2024-0318, and VND-2024-0501 have the broadest exposure."
    }
  }
}
```

---

### Step 6 — Export

```json
POST /export
{
  "run_id": "run-c3d4e5f6",
  "destination": {
    "type": "s3",
    "uri": "s3://compliance-reports/audits/2026-Q1/uncapped-liability-clauses.json"
  },
  "include_provenance": true
}
```

```json
{
  "export_id": "export-1a2b3c4d",
  "status": "complete",
  "destination_uri": "s3://compliance-reports/audits/2026-Q1/uncapped-liability-clauses.json",
  "provenance_uri": "s3://compliance-reports/audits/2026-Q1/uncapped-liability-clauses.provenance.json"
}
```

The provenance file records the full chain from plan to export, including the principal,
timestamp, destination, and run ID. For compliance data, this is required by
`policies/examples/restricted-dataset.cedar` — any export of clearance-level resources
must include `include_provenance: true` and a `hitl_approval_id`.

---

## Safety boundaries active in this scenario

| Stage | Layer | What fired |
|-------|-------|-----------|
| Probe | Guardrails | `ApplyGuardrail` anonymized `counterparty` values (NAME entity → ANONYMIZE) in sample rows |
| Plan | Guardrails | Injection detection (HIGH) evaluated `objective` before generating S3 Select SQL |
| Excavate | Guardrails | `ApplyGuardrail` scanned 23 result rows; anonymized personal names in `clause_text` |
| Export | Cedar | Verified `s3://compliance-reports/` is in the principal's `approved_export_targets` |
| Export | Cedar | `forbid` clause blocks export to any destination not in the allowlist |
| Export | Guardrails | Final `ApplyGuardrail` scan on the full payload before writing to S3 |

For datasets classified as restricted (PHI, clearance-level data), add the
`restricted-dataset.cedar` policy overlay, which additionally requires a
`hitl_approval_id` on excavate and export calls.
See [policies/examples/restricted-dataset.cedar](../../policies/examples/restricted-dataset.cedar).

---

## Setup

1. **Deploy clAWS stacks** — follow [docs/getting-started.md](../../docs/getting-started.md).

2. **Create the S3 bucket:**

   ```bash
   aws s3 mb s3://legal-corpus-prod --region us-east-1
   ```

3. **Prepare the Parquet file.** Use pandas or pyarrow to create a Parquet file with the
   schema above:

   ```python
   import pandas as pd

   df = pd.DataFrame([
       {"doc_id": "VND-2024-0047", "contract_type": "SaaS", "counterparty": "Acme Corp",
        "section": "8.2", "clause_text": "Vendor shall indemnify...",
        "clause_type": "indemnification", "liability_cap": None, "effective_date": "2024-01-15"},
       # ... more rows
   ])
   df.to_parquet("all-contracts-2024.parquet", index=False)
   ```

4. **Upload the file:**

   ```bash
   aws s3 cp all-contracts-2024.parquet \
     s3://legal-corpus-prod/contracts/extracted/all-contracts-2024.parquet
   ```

5. **Tag the bucket** with `claws:space = legal-docs`:

   ```bash
   aws s3api put-bucket-tagging \
     --bucket legal-corpus-prod \
     --tagging 'TagSet=[{Key=claws:space,Value=legal-docs}]'
   ```

6. **Update the Cedar policy** for the compliance agent principal:
   - Add `"s3://legal-corpus-prod/contracts/extracted/all-contracts-2024.parquet"` to
     `approved_sources`
   - Add `"legal-docs"` to `approved_spaces`
   - Add `"s3://compliance-reports/"` to `approved_export_targets`
   - See [docs/user-guide.md](../../docs/user-guide.md#cedar-policy-authoring-guide) for
     the policy format.

7. **IAM permissions:** The clAWS Lambda IAM role needs `s3:GetObject` on the source bucket.
   This is already granted to `CLAWS_RUNS_BUCKET` in `ClawsToolsStack` — add the legal
   corpus bucket separately:

   ```python
   lambda_role.add_to_policy(iam.PolicyStatement(
       effect=iam.Effect.ALLOW,
       actions=["s3:GetObject"],
       resources=["arn:aws:s3:::legal-corpus-prod/*"],
   ))
   ```
