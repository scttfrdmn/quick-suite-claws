# Safety Model

clAWS uses two independent, complementary enforcement layers. Neither alone
is sufficient. Together they cover both the structural and semantic threat
surfaces.

## Layer 1: Cedar (AgentCore Policy) — Structural enforcement

**What it gates:** Permissions, cost bounds, source allowlists, destination
allowlists, plan linkage.

**Evaluation:** Deterministic. Evaluated at the AgentCore Gateway boundary
before any tool executes. No ML, no probabilistic behavior.

**Key properties:**

- Source allowlists per principal
- Byte-scan limits per principal
- Plan-to-excavation linkage (plan_id required)
- Export destination allowlists
- HITL approval requirements for cross-domain operations

See `policies/default.cedar` for the base policy and `policies/examples/`
for domain-specific examples.

## Layer 2: Bedrock Guardrails — Semantic enforcement

**What it gates:** Content meaning — PII, PHI, prompt injection, denied
topics, toxicity, contextual grounding.

**Evaluation:** ML-based + rule-based (regex, word filters). Applied at
multiple points in the pipeline via two integration modes.

### Integration modes

**Mode 1: Model guardrails** — Attached to `InvokeModel` calls in the
`plan` and `refine` tools. Filters both input (objective) and output
(generated query/summary).

**Mode 2: `ApplyGuardrail` API** — Standalone content scanning. Used on
`probe` sample data, `excavate` results, and `export` payloads. No LLM
involved.

### Attachment points

| Tool | Mode | Primary filters |
|------|------|-----------------|
| discover | — | No guardrail (structured I/O, metadata only) |
| probe | ApplyGuardrail | PII/PHI on sample rows |
| plan | Model guardrail | Injection, denied topics, PII on query |
| excavate | ApplyGuardrail | PII/PHI on result payload |
| refine | Model guardrail | Grounding check on summaries |
| export | ApplyGuardrail | Final content gate before materialization |

### Why both layers?

Cedar cannot read content — it can enforce "only query approved sources" but
not "don't return SSNs." Guardrails cannot enforce structure — it can detect
PII but not verify that a byte-scan limit is within bounds. An attacker
would need to defeat both simultaneously.

## Defense in depth

Beyond Cedar and Guardrails, clAWS enforces constraints at the backend level:

- **Athena workgroup limits** — byte-scan caps enforced by Athena itself
- **Read-only IAM roles** — Lambda execution roles have no write permissions
- **Query validation** — SQL parsed for mutations before plan approval
- **Result size caps** — output bounded before return to agent
- **Full audit trail** — every tool call logged with inputs, outputs, cost,
  and guardrail trace

See `guardrails/` for configuration files and `infra/cdk/stacks/guardrails_stack.py`
for the CDK deployment.
