# Example: Document Mining

End-to-end example of using clAWS to excavate structured data from
documents stored in S3.

## Scenario

A compliance agent needs to find specific clauses across a corpus of
contracts stored as JSON-extracted documents in S3.

## Pipeline

```
discover("contract clauses indemnification", scope=legal-docs)
  → s3:legal-corpus/contracts/

probe(s3:legal-corpus/contracts/, mode=schema_and_samples)
  → JSON schema with fields: doc_id, section, clause_text, parties

plan("Find all indemnification clauses with uncapped liability")
  → S3 Select SQL over JSON documents

excavate(plan_id=plan-...)
  → matching clauses with document references

refine(operations=[dedupe, summarize])
  → deduplicated clauses with risk summary

export(destination=s3://compliance-reports/)
```

## Status

TODO: Implement S3 Select executor in `tools/excavate/executors/s3_select.py`
