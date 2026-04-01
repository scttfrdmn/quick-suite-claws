# Getting Started with clAWS

This guide takes you from zero to a confirmed, working excavation. Every step has a
verification command so you know it worked before moving on.

---

## Prerequisites

### 1. AWS account and IAM permissions

Your AWS credentials need permissions to deploy and invoke the clAWS stacks. At minimum:

```
bedrock:*
lambda:*
s3:*
dynamodb:*
athena:*
glue:*
logs:*
iam:PassRole
iam:CreateRole
iam:AttachRolePolicy
cloudformation:*
```

Verify your credentials are configured:

```bash
aws sts get-caller-identity
# Expected output: {"UserId": "...", "Account": "123456789012", "Arn": "arn:aws:iam::..."}
```

### 2. AWS CDK v2

```bash
npm install -g aws-cdk
cdk --version   # should print 2.x.x
```

### 3. Python 3.12 or later

```bash
python --version   # should print Python 3.12.x or later
```

### 4. CDK bootstrap

CDK requires a bootstrap stack in each account/region you deploy to. If you haven't done
this for your target region:

```bash
cdk bootstrap aws://YOUR_ACCOUNT_ID/YOUR_REGION
# e.g.: cdk bootstrap aws://123456789012/us-east-1
```

---

## Install

```bash
git clone https://github.com/scttfrdmn/claws.git
cd claws
pip install -e ".[dev,cdk]"
```

Verify the install by running the test suite:

```bash
pytest tools/ -q
# Expected: 105 passed in N.Ns
```

If this fails, check your Python version and that all dependencies installed correctly.

---

## Deploy

From the `infra/cdk` directory:

```bash
cd infra/cdk
cdk deploy --all --require-approval never
```

CDK deploys the five stacks in dependency order:

| Stack | What it creates |
|-------|----------------|
| `ClawsStorageStack` | S3 buckets (`claws-runs`, `claws-athena-results`), DynamoDB tables (`claws-plans`, `claws-schemas`) |
| `ClawsGuardrailsStack` | Bedrock Guardrail with content filters, PII detection, injection detection |
| `ClawsToolsStack` | Six Lambda functions, shared IAM role, Athena read-only workgroup |
| `ClawsGatewayStack` | AgentCore Gateway, one endpoint per tool Lambda |
| `ClawsPolicyStack` | Cedar policy deployment, policy-to-gateway association |

The deploy takes 5–10 minutes. When it finishes, CDK prints the stack outputs including
the Gateway ID and Guardrail ID — save these.

### Verify deployment

**1. Lambda is reachable:**

```bash
aws lambda invoke \
  --function-name claws-discover \
  --payload '{"query": "test", "scope": {"domains": ["athena"], "spaces": []}}' \
  /tmp/discover-out.json
cat /tmp/discover-out.json
# Expected: {"statusCode": 200, "body": "{\"sources\": []}"}
```

**2. DynamoDB tables exist:**

```bash
aws dynamodb list-tables --query 'TableNames[?contains(@, `claws`)]'
# Expected: ["claws-plans", "claws-schemas"]
```

**3. S3 buckets exist:**

```bash
aws s3 ls | grep claws
# Expected: two lines containing claws-runs and claws-athena-results
```

---

## Your first excavation

This walkthrough follows the genomics scenario from
[examples/genomics-excavation](../examples/genomics-excavation/).

**Before you start:** create an Athena table `oncology.variant_index` with columns
`gene`, `cohort`, `variant_class`, `classification`, `chromosome`, `position`,
`ref_allele`, `alt_allele`, `n_samples`, `confidence_score`, `last_updated`, `source_study`.
Tag it in Glue with `claws:space = genomics-shared`. See the example README for a sample
data file.

### Step 1 — Discover the data source

Find available sources matching your query terms.

**Request:**
```json
{
  "query": "BRCA1 pathogenic variant annotations",
  "scope": {
    "domains": ["athena"],
    "spaces": ["genomics-shared"]
  },
  "limit": 10
}
```

**Response:**
```json
{
  "sources": [
    {
      "id": "athena:oncology.variant_index",
      "kind": "table",
      "confidence": 0.92,
      "reason": "Table name and Glue tag match query terms"
    }
  ]
}
```

Carry forward: `sources[0].id` — you'll use this as `source_id` in the next steps.

### Step 2 — Probe the source

Inspect the schema and get sample rows before writing a query.

**Request:**
```json
{
  "source_id": "athena:oncology.variant_index",
  "mode": "schema_and_samples",
  "sample_rows": 5
}
```

**Response:**
```json
{
  "source_id": "athena:oncology.variant_index",
  "schema": {
    "database": "oncology",
    "table": "variant_index",
    "columns": [
      {"name": "gene",               "type": "string",  "partition_key": true},
      {"name": "cohort",             "type": "string",  "partition_key": true},
      {"name": "variant_class",      "type": "string"},
      {"name": "classification",     "type": "string"},
      {"name": "chromosome",         "type": "string"},
      {"name": "position",           "type": "bigint"},
      {"name": "ref_allele",         "type": "string"},
      {"name": "alt_allele",         "type": "string"},
      {"name": "n_samples",          "type": "integer"},
      {"name": "confidence_score",   "type": "double"},
      {"name": "last_updated",       "type": "date"},
      {"name": "source_study",       "type": "string"}
    ]
  },
  "samples": [
    {"gene": "BRCA1", "cohort": "TCGA-OV", "variant_class": "SNV",
     "classification": "pathogenic", "chromosome": "17", "position": 43094692,
     "n_samples": 12, "confidence_score": 0.97}
  ],
  "row_count_estimate": 4200000,
  "size_bytes_estimate": 2684354560,
  "cost_estimates": {
    "full_scan_cost": "$0.63",
    "with_gene_partition": "$0.01",
    "note": "Partition on gene=BRCA1 reduces scan to ~5MB"
  }
}
```

Carry forward: the schema (the plan tool uses it), and the cost estimates.

> **Safety note:** The sample rows are scanned by `ApplyGuardrail` before being returned.
> If any row contained PHI (patient ID, date of birth, etc.), this call would return
> `status: blocked` rather than exposing the data.

### Step 3 — Plan the excavation

Translate your objective into a concrete, reviewable query. This is the only step with
free-text input; everything after this is deterministic.

**Request:**
```json
{
  "objective": "Count pathogenic BRCA1 variants by cohort, grouped by variant class",
  "source_id": "athena:oncology.variant_index",
  "constraints": {
    "max_cost_dollars": 1.00,
    "max_bytes_scanned": 5368709120,
    "read_only": true
  }
}
```

**Response:**
```json
{
  "plan_id": "plan-a1b2c3d4",
  "status": "ready",
  "steps": [
    {
      "input": {
        "source_id": "athena:oncology.variant_index",
        "query": "SELECT cohort, variant_class, COUNT(*) AS n FROM variant_index WHERE gene = 'BRCA1' AND classification = 'pathogenic' GROUP BY cohort, variant_class ORDER BY n DESC",
        "query_type": "athena_sql"
      },
      "description": "Aggregate pathogenic BRCA1 variants by cohort and variant class"
    }
  ],
  "estimated_cost": "$0.24",
  "estimated_bytes_scanned": 1048576000,
  "output_schema": [
    {"name": "cohort",        "type": "string"},
    {"name": "variant_class", "type": "string"},
    {"name": "n",             "type": "bigint"}
  ]
}
```

Carry forward: `plan_id` and the entire `steps[0].input` object — you'll pass it verbatim
to excavate.

> **Status values:** `ready` means the plan passed all validation checks. `rejected` means
> the SQL validator blocked a mutation (UPDATE/DELETE/DROP) or a multi-statement attack.
> `blocked` means Bedrock Guardrails flagged the objective for injection or a denied topic.

### Step 4 — Excavate

Execute the exact query from the plan. Pass `steps[0].input` fields directly.

> **Important:** The `query` string must match the stored plan exactly. If you change even
> one character, excavate returns HTTP 403 "Query does not match stored plan." This
> bait-and-switch protection prevents a query from being modified after Cedar approves the
> plan but before execution.

**Request:**
```json
{
  "plan_id": "plan-a1b2c3d4",
  "source_id": "athena:oncology.variant_index",
  "query": "SELECT cohort, variant_class, COUNT(*) AS n FROM variant_index WHERE gene = 'BRCA1' AND classification = 'pathogenic' GROUP BY cohort, variant_class ORDER BY n DESC",
  "query_type": "athena_sql",
  "constraints": {
    "max_bytes_scanned": 5368709120,
    "read_only": true
  }
}
```

**Response:**
```json
{
  "run_id": "run-x7y8z9ab",
  "status": "complete",
  "rows_returned": 42,
  "bytes_scanned": 237123584,
  "cost": "$0.22",
  "result_uri": "s3://claws-runs/run-x7y8z9ab/result.json",
  "result_preview": [
    {"cohort": "TCGA-OV",  "variant_class": "SNV",   "n": 187},
    {"cohort": "TCGA-BRCA","variant_class": "SNV",   "n": 143},
    {"cohort": "TCGA-OV",  "variant_class": "INDEL", "n":  62},
    {"cohort": "TCGA-BRCA","variant_class": "INDEL", "n":  41},
    {"cohort": "TCGA-LUAD","variant_class": "SNV",   "n":  38}
  ]
}
```

Carry forward: `run_id`.

### Step 5 — Refine

Dedupe, rank, and summarize the results.

**Request:**
```json
{
  "run_id": "run-x7y8z9ab",
  "operations": ["dedupe", "rank", "summarize"],
  "top_k": 25,
  "output_format": "json"
}
```

**Response:**
```json
{
  "run_id": "run-x7y8z9ab",
  "refined_uri": "s3://claws-runs/run-x7y8z9ab/refined.json",
  "manifest": {
    "operations_applied": ["dedupe", "rank", "summarize"],
    "rows_in": 42,
    "rows_out": 25,
    "dedupe": {"duplicates_removed": 4},
    "rank": {"ranked_by": "n", "order": "desc"},
    "summarize": {"model": "amazon.nova-lite-v1:0", "grounding_check": "passed"}
  }
}
```

### Step 6 — Export

Materialize results to an S3 destination with a provenance chain.

**Request:**
```json
{
  "run_id": "run-x7y8z9ab",
  "destination": {
    "type": "s3",
    "uri": "s3://your-output-bucket/brca1-pathogenic-summary.json"
  },
  "include_provenance": true
}
```

**Response:**
```json
{
  "export_id": "export-fe012345",
  "status": "complete",
  "destination_uri": "s3://your-output-bucket/brca1-pathogenic-summary.json",
  "provenance_uri": "s3://your-output-bucket/brca1-pathogenic-summary.provenance.json"
}
```

### Verify results

```bash
# Download and inspect the results
aws s3 cp s3://your-output-bucket/brca1-pathogenic-summary.json .
cat brca1-pathogenic-summary.json

# Download and inspect the provenance chain
aws s3 cp s3://your-output-bucket/brca1-pathogenic-summary.provenance.json .
cat brca1-pathogenic-summary.provenance.json
```

The provenance file records the full chain:

```json
{
  "export_timestamp": "2026-04-01T18:30:00.000000+00:00",
  "principal": "research-agent-prod",
  "run_id": "run-x7y8z9ab",
  "destination": {
    "type": "s3",
    "uri": "s3://your-output-bucket/brca1-pathogenic-summary.json"
  },
  "chain": {
    "note": "Full provenance chain: plan → query → raw result → refinement → export",
    "run_id": "run-x7y8z9ab"
  }
}
```

---

## Where to go next

- **Write Cedar policies for your team** — see the
  [Cedar policy authoring guide](user-guide.md#cedar-policy-authoring-guide) in the user guide.
- **Add your own data source** — tag a Glue table with `claws:space = your-space`, add the
  space to the principal's `approved_spaces` in your Cedar policy, and `discover` will find it.
- **OpenSearch example** — see [examples/log-analysis](../examples/log-analysis/) for a
  complete OpenSearch DSL pipeline.
- **S3 Select example** — see [examples/document-mining](../examples/document-mining/) for a
  complete S3 Select / Parquet pipeline.
- **Architecture details** — see [docs/architecture.md](architecture.md) for CDK stack
  internals and storage layout.
