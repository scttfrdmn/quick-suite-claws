# Example: Log Analysis

End-to-end example of using clAWS to excavate application logs
stored in OpenSearch.

## Scenario

An operations agent needs to find error patterns across services
over the past 24 hours. Logs are indexed in OpenSearch with fields
for timestamp, service, level, message, and trace_id.

## Pipeline

```
discover("application error logs", scope=ops-logs)
  → opensearch:prod-logs-2025

probe(opensearch:prod-logs-2025, mode=schema_and_samples)
  → schema with 8 fields, ~2M docs/day

plan("Find top error patterns by service in the last 24 hours")
  → OpenSearch DSL aggregation query

excavate(plan_id=plan-...)
  → aggregation results: error counts by service and message pattern

refine(operations=[rank, summarize])
  → ranked error patterns with LLM-generated summary

export(destination=s3://ops-reports/daily/)
```

## Status

TODO: Implement OpenSearch executor in `tools/excavate/executors/opensearch.py`
