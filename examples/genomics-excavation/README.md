# Example: Genomics Excavation

End-to-end example of using clAWS to query genomic variant data
in Athena.

## Scenario

A research agent needs to find BRCA1 pathogenic variants across
study cohorts. The data lives in an Athena table with partition
keys on `gene` and `cohort`.

## Pipeline

```
discover("genomics variant annotations", scope=genomics-shared)
  → athena:oncology.variant_index

probe(athena:oncology.variant_index, mode=schema_and_samples)
  → schema with 12 columns, partitioned on gene + cohort

plan("Count pathogenic BRCA1 variants by cohort")
  → SELECT cohort, variant_class, COUNT(*) AS n
    FROM variant_index
    WHERE gene = 'BRCA1' AND classification = 'pathogenic'
    GROUP BY cohort, variant_class
  → estimated cost: $0.24, estimated rows: 45

excavate(plan_id=plan-a1b2c3)
  → 42 rows, $0.22 actual cost

refine(run_id=run-x7y8z9, operations=[dedupe, rank_by_n, summarize])
  → top 25 results with summary

export(run_id=run-refined, destination=s3://research-outputs/)
  → s3://research-outputs/brca1-summary.json + provenance
```

## Safety boundaries active

- Cedar: source in genomics-shared space, byte limit 5GB, read_only
- Guardrails: PII scan on samples and results, injection detection on objective
- Athena workgroup: 5GB byte-scan cap enforced at engine level
- Plan validation: SQL checked for mutations before approval

## Setup

1. Deploy clAWS stacks
2. Create Athena table `oncology.variant_index` with sample data
3. Add table to Glue catalog with `claws:space = genomics-shared` tag
4. Run the agent session (see `agent.py`)
