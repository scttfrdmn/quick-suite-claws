# Example: Genomics Excavation

End-to-end example of using clAWS to query genomic variant data stored in Athena.
An oncology research agent identifies BRCA1 pathogenic variants by cohort, deduplicates
and ranks the results, then exports them to S3 with a full provenance chain.

---

## Scenario

**Agent:** oncology research agent  
**Data source:** Athena table `oncology.variant_index` in AWS Glue catalog  
**Goal:** Count pathogenic BRCA1 variants by cohort and variant class

Table schema:

| Column | Type | Description |
|--------|------|-------------|
| `gene` | string | Gene symbol (partition key) |
| `cohort` | string | Study cohort ID (partition key) |
| `chromosome` | string | Chromosome identifier |
| `position` | int | Genomic position (GRCh38) |
| `ref_allele` | string | Reference allele |
| `alt_allele` | string | Alternate allele |
| `classification` | string | `pathogenic`, `likely_pathogenic`, `vus`, `benign` |
| `variant_class` | string | `SNP`, `indel`, `CNV`, `SV` |
| `sample_count` | int | Number of samples with this variant |
| `af_global` | float | Global allele frequency |
| `af_cohort` | float | Cohort-specific allele frequency |
| `evidence_score` | float | ClinVar / internal evidence score |

Partitioned on `gene` and `cohort` — partition pruning reduces scan cost significantly.

---

## Pipeline

### Step 1 — Discover

```json
POST /discover
{
  "query": "genomic variant annotations oncology",
  "scope": {
    "domains": ["athena"],
    "spaces": ["genomics-shared"]
  }
}
```

Response (abbreviated):

```json
{
  "sources": [
    {
      "source_id": "athena:oncology.variant_index",
      "description": "Variant annotation index — partitioned on gene + cohort",
      "confidence": 0.94,
      "tags": { "claws:space": "genomics-shared", "claws:domain": "athena" }
    }
  ]
}
```

---

### Step 2 — Probe

```json
POST /probe
{
  "source_id": "athena:oncology.variant_index",
  "mode": "schema_and_samples"
}
```

Response (abbreviated):

```json
{
  "schema": {
    "database": "oncology",
    "table": "variant_index",
    "columns": ["gene", "cohort", "chromosome", "position", "classification", "..."],
    "partition_keys": ["gene", "cohort"],
    "size_bytes_estimate": 48318382080
  },
  "samples": [
    { "gene": "BRCA1", "cohort": "TCGA-OV", "classification": "pathogenic", "variant_class": "SNP" },
    { "gene": "TP53",  "cohort": "TCGA-BRCA", "classification": "pathogenic", "variant_class": "SNP" }
  ]
}
```

probe caches the schema. plan will fail with 422 if probe hasn't run first.

---

### Step 3 — Plan

```json
POST /plan
{
  "source_id": "athena:oncology.variant_index",
  "objective": "Count pathogenic BRCA1 variants by cohort and variant class",
  "constraints": {
    "max_cost_dollars": 1.00,
    "max_bytes_scanned": 5368709120,
    "read_only": true
  }
}
```

The `plan` tool calls Claude via Bedrock Guardrails and returns a concrete,
reviewable SQL query — nothing is executed yet:

```json
{
  "plan_id": "plan-a1b2c3d4",
  "status": "ready",
  "steps": [
    {
      "tool": "claws.excavate",
      "input": {
        "plan_id": "plan-a1b2c3d4",
        "source_id": "athena:oncology.variant_index",
        "query": "SELECT cohort, variant_class, COUNT(*) AS n FROM oncology.variant_index WHERE gene = 'BRCA1' AND classification = 'pathogenic' GROUP BY cohort, variant_class ORDER BY n DESC",
        "query_type": "athena_sql"
      }
    }
  ],
  "estimated_cost": "$0.24",
  "estimated_bytes_scanned": 51380224
}
```

The agent reviews the plan before approving execution. Cedar validates the concrete
query at the Gateway boundary — the gene partition filter and byte-scan limit are
enforced structurally, not just by convention.

---

### Step 4 — Excavate

```json
POST /excavate
{
  "plan_id": "plan-a1b2c3d4",
  "source_id": "athena:oncology.variant_index",
  "query": "SELECT cohort, variant_class, COUNT(*) AS n FROM oncology.variant_index WHERE gene = 'BRCA1' AND classification = 'pathogenic' GROUP BY cohort, variant_class ORDER BY n DESC",
  "query_type": "athena_sql"
}
```

excavate validates that the submitted query matches the stored plan (bait-and-switch
protection), then runs it on Athena. Guardrails scans the result rows for PII/PHI
before returning them.

```json
{
  "run_id": "run-x7y8z9a1",
  "status": "complete",
  "rows_returned": 42,
  "bytes_scanned": 49807360,
  "cost": "$0.22",
  "result_uri": "s3://claws-runs/run-x7y8z9a1/result.json"
}
```

---

### Step 5 — Refine

```json
POST /refine
{
  "run_id": "run-x7y8z9a1",
  "operations": ["dedupe", "rank", "summarize"]
}
```

```json
{
  "run_id": "run-b2c3d4e5",
  "manifest": {
    "operations": [
      { "op": "dedupe",    "rows_before": 42, "rows_after": 38 },
      { "op": "rank",      "rows_before": 38, "rows_after": 38 },
      { "op": "summarize", "rows_before": 38, "rows_after": 38 }
    ]
  },
  "summary": "BRCA1 pathogenic variants are most prevalent in TCGA-OV (18 variants) and TCGA-BRCA (14 variants). SNPs account for 71% of pathogenic calls; indels 22%."
}
```

---

### Step 6 — Export

```json
POST /export
{
  "run_id": "run-b2c3d4e5",
  "destination": {
    "type": "s3",
    "uri": "s3://research-outputs/brca1-cohort-summary.json"
  },
  "include_provenance": true
}
```

```json
{
  "status": "complete",
  "destination_uri": "s3://research-outputs/brca1-cohort-summary.json",
  "provenance_uri": "s3://research-outputs/brca1-cohort-summary.provenance.json",
  "rows_exported": 38
}
```

The provenance file records the full chain: plan_id → run_id → refine run_id →
export, with timestamps, byte counts, and the Cedar policy version that governed
the query.

---

## Safety boundaries active

| Layer | What it enforces |
|-------|-----------------|
| Cedar (Gateway) | Source must be in `genomics-shared` space; byte scan ≤ 5 GB; `read_only = true`; submitted query must match stored plan |
| Bedrock Guardrails | PII/PHI detection on probe samples and excavate results; prompt injection detection on the objective |
| Athena workgroup | 5 GB byte-scan hard cap enforced at the engine level — independent of clAWS |
| SQL validator | Mutation detection (INSERT/UPDATE/DELETE/DROP) before plan approval |

---

## Setup

### 1. Deploy clAWS

```bash
cd infra/cdk
cdk deploy --all
```

### 2. Create the Athena table

```sql
CREATE DATABASE IF NOT EXISTS oncology;

CREATE EXTERNAL TABLE oncology.variant_index (
  chromosome     STRING,
  position       INT,
  ref_allele     STRING,
  alt_allele     STRING,
  classification STRING,
  variant_class  STRING,
  sample_count   INT,
  af_global      DOUBLE,
  af_cohort      DOUBLE,
  evidence_score DOUBLE
)
PARTITIONED BY (gene STRING, cohort STRING)
STORED AS PARQUET
LOCATION 's3://your-genomics-bucket/variant-index/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

MSCK REPAIR TABLE oncology.variant_index;
```

### 3. Tag the table for discovery

```bash
aws glue tag-resource \
  --resource-arn arn:aws:glue:us-east-1:123456789012:table/oncology/variant_index \
  --tags-to-add '{"claws:space": "genomics-shared", "claws:domain": "athena"}'
```

### 4. Apply the example Cedar policy

```bash
cp policies/examples/research-team.cedar policies/active/
# Re-deploy ClawsPolicyStack to pick up the new policy
cdk deploy ClawsPolicyStack
```

The `research-team.cedar` policy permits `genomics-shared` sources with a 5 GB
scan limit for principals in the `research` group.
