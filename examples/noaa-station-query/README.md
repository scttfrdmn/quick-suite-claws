# clAWS Example: Query Your Data Catalog

A self-contained example of the full clAWS pipeline — discover, probe, plan,
excavate, refine, export — against a single Athena-backed Glue table.

## What This Shows

The six-step pipeline in action on structured tabular data:

1. **Discover** — find registered tables in the Athena domain
2. **Probe** — inspect schema and sample rows (with PII scan)
3. **Plan** — translate a plain-English objective into a concrete SQL query
4. **Excavate** — execute the query; results are stored in S3
5. **Refine** — deduplicate the result set
6. **Export** — write refined results and a provenance chain to S3

## Data

Uses `claws_e2e.sample_data` — the Glue table created by the E2E test fixtures
(three columns: `id`, `name`, `value`). Replace `source_id` in `scenario.yaml`
with any table registered in your Glue catalog and update the objective to
match your schema.

For a realistic NOAA weather station dataset, register a Glue table backed by
the NOAA GHCN public S3 bucket (`s3://noaa-ghcn-pds/`) and update:

```yaml
source_id: "athena:your_database.noaa_ghcn_stations"
objective: "List the 10 stations with the most measurements, showing station_id, name, and measurement_count."
```

## Prerequisites

- clAWS stacks deployed (`ClawsToolsStack`, `ClawsStorageStack`, `ClawsGuardrailsStack`)
- At least one Glue table registered in the `claws-readonly` Athena workgroup
- `AWS_PROFILE` pointing to the deployment account

## Running

```bash
# Via the capstone scenario runner:
AWS_PROFILE=aws python3 -m pytest tests/scenarios/ -v -m scenario -k noaa-station-query

# Or invoke steps manually with the AWS CLI:
aws lambda invoke --function-name claws-plan \
  --payload '{"source_id":"athena:claws_e2e.sample_data","objective":"List top 5 rows by value."}' \
  --cli-binary-format raw-in-base64-out /tmp/plan.json && cat /tmp/plan.json | jq .body | jq -r . | jq .
```
