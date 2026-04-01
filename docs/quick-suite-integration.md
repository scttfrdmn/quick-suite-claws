# Quick Suite Integration

Amazon Quick Suite is an **optional** operator and workflow surface for clAWS.
clAWS runs entirely on AgentCore — Quick Suite adds scheduling, HITL workflows,
dashboards, and chat interfaces on top.

## When to use Quick Suite

Quick Suite is valuable when you need:

- **Scheduled excavations** — recurring queries via Quick Flows
- **Human-in-the-loop** — approval workflows via Quick Automate
- **Operator dashboards** — cost tracking, audit views, excavation metrics
- **Chat interface** — ad-hoc excavations via Quick Chat with clAWS as tool source
- **Research workflows** — agent-driven exploration via Quick Research

## Architecture

```
┌──────────────────────────────────────────────┐
│                 Quick Suite                    │
│                                               │
│  ┌─────────┐ ┌──────────┐ ┌───────────────┐ │
│  │  Flows   │ │ Automate │ │ Chat/Research │ │
│  │(schedule)│ │  (HITL)  │ │  (ad-hoc)     │ │
│  └────┬─────┘ └────┬─────┘ └──────┬────────┘ │
│       │             │              │           │
└───────┼─────────────┼──────────────┼───────────┘
        │             │              │
        ▼             ▼              ▼
┌──────────────────────────────────────────────┐
│           AgentCore Gateway                   │
│         Cedar Policy Enforcement              │
│                                               │
│  discover → probe → plan → excavate → export  │
│                                               │
└──────────────────────────────────────────────┘
```

Quick Suite calls clAWS tools through the same AgentCore Gateway as any
other agent. No special integration — Quick Suite is just another client
with its own principal and Cedar policy.

## Quick Flows — scheduled excavations

```yaml
# Example: weekly genomics variant summary
flow:
  name: weekly-brca1-report
  schedule: "cron(0 8 ? * MON *)"  # Every Monday 8am
  steps:
    - tool: claws.discover
      input:
        query: "BRCA1 variant annotations"
        scope: {domains: [athena], spaces: [genomics-shared]}
    - tool: claws.probe
      input:
        source_id: "{{steps[0].sources[0].id}}"
        mode: schema_and_samples
    - tool: claws.plan
      input:
        objective: "Count pathogenic BRCA1 variants by cohort, last 7 days"
        source_id: "{{steps[0].sources[0].id}}"
        constraints: {max_cost_dollars: 2.00, read_only: true}
    - tool: claws.excavate
      input: "{{steps[2].steps[0].input}}"
    - tool: claws.refine
      input:
        run_id: "{{steps[3].run_id}}"
        operations: [dedupe, rank_by_n, summarize]
    - tool: claws.export
      input:
        run_id: "{{steps[4].run_id}}"
        destination: {type: s3, uri: "s3://reports/brca1-weekly/"}
```

## Quick Automate — HITL approval

For sensitive datasets (restricted-dataset Cedar policy), the plan step
surfaces to a human operator in Quick Automate:

1. Agent calls `claws.plan` → plan is generated
2. Quick Automate pauses the workflow and sends plan to approver
3. Approver reviews the concrete SQL, estimated cost, output schema
4. If approved → `hitl_approval_id` is injected → `claws.excavate` proceeds
5. If rejected → workflow terminates with rejection reason

## Quick dashboards

Quick Suite dashboards can visualize:

- Excavation volume over time
- Cost by team/dataset/tool
- Guardrail intervention rates
- Plan rejection rates and reasons
- Export destinations and provenance chains
- HITL approval latency

Data source: clAWS audit logs → OpenSearch → Quick dashboard.
