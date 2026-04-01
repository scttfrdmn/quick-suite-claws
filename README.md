# clAWS — Controlled Excavation Tools for Agents

**Safe excavation tools on Amazon Bedrock AgentCore.**

clAWS is an open-source reference architecture for building enterprise-grade
data excavation tool planes that agents can call safely. It demonstrates how to
give agents real access to real data — S3, Athena, OpenSearch, DynamoDB, RDS —
while keeping every action auditable, cost-bounded, and policy-gated.

## Architecture

clAWS is a secure excavation tool plane on Amazon Bedrock AgentCore,
with Cedar policies for structural enforcement, Bedrock Guardrails for
content safety, and Amazon Quick Suite as an optional operator and
workflow surface.

### Core principle: separate reasoning from execution

LLM reasoning never happens inside a tool. Tools are deterministic executors.
The agent session reasons, plans, and decides; tools accept concrete, auditable
inputs and return concrete outputs.

```
Agent session (reasoning)          Tool plane (execution)
┌─────────────────────┐            ┌─────────────────────┐
│ "Find BRCA1          │            │                     │
│  pathogenic variants  │  plan ──▶ │  plan tool returns   │
│  by cohort"           │           │  concrete query +    │
│                       │           │  cost estimate       │
│  reviews plan ───────▶│           │                     │
│                       │  exec ──▶ │  excavate runs the   │
│  receives results     │           │  exact query, nothing│
│                       │           │  else                │
└─────────────────────┘            └─────────────────────┘
         │                                    │
         │         Cedar + Guardrails         │
         └──────── enforced here ─────────────┘
```

## Tool families

| Tool | Purpose | Policy gate |
|------|---------|-------------|
| `claws.discover` | Find data sources in approved domains | Cedar: scope allowlist |
| `claws.probe` | Inspect schema, samples, cost estimates | Cedar: source allowlist, Guardrails: PII scan on samples |
| `claws.plan` | Translate objective → concrete query (LLM) | Guardrails: injection + denied topics, Cedar: constraints |
| `claws.excavate` | Execute concrete query | Cedar: source + bounds + plan linkage |
| `claws.refine` | Dedupe, rank, summarize results | Guardrails: grounding check on summaries |
| `claws.export` | Materialize to S3/event bus | Cedar: destination allowlist, Guardrails: final content scan |

## Safety model

Two independent, complementary enforcement layers:

- **Cedar (AgentCore Policy)** — Structural/deterministic. Permissions, cost bounds, source allowlists. Evaluated at Gateway boundary before execution.
- **Bedrock Guardrails** — Semantic/content. PII/PHI detection, prompt injection, denied topics, contextual grounding. Applied at LLM I/O and via ApplyGuardrail API on data paths.

## Quick start

```bash
# Prerequisites: AWS CDK, Python 3.12+, AWS credentials

cd infra/cdk
pip install -r requirements.txt
cdk deploy ClawsToolsStack ClawsGatewayStack ClawsPolicyStack ClawsGuardrailsStack
```

## Project structure

```
claws/
├── api/                    OpenAPI spec for Gateway
├── policies/               Cedar policies
├── guardrails/             Bedrock Guardrail configs
├── tools/                  Lambda handlers per tool
│   ├── discover/
│   ├── probe/
│   ├── plan/               Includes SQL validator + cost estimator
│   ├── excavate/           Backend executors (Athena, OpenSearch, S3 Select)
│   ├── refine/
│   └── export/
├── infra/                  CDK stacks + AgentCore config
└── examples/               End-to-end excavation examples
```

## License

Apache 2.0
