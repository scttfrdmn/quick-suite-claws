# Capstone Deployment Guide

clAWS supports two deployment modes for the AgentCore Gateway:

- **Standalone** — clAWS creates its own Gateway (default)
- **Capstone** — clAWS attaches to an existing shared Gateway (e.g. from quick-suite-router)

## Standalone deployment (default)

```bash
cdk deploy --all
```

This creates a new `claws-gateway` AgentRuntime and registers all six tool endpoints.

## Capstone deployment (shared Gateway)

When operating within a Quick Suite capstone environment, you can reuse an existing
AgentCore Gateway instead of creating a new one. Pass the gateway ID via CDK context:

```bash
cdk deploy --all -c CLAWS_GATEWAY_ID=agr-abc123
```

Where `agr-abc123` is the `agentRuntimeId` of the shared Gateway (e.g. from
`quick-suite-router`'s CloudFormation outputs).

In Capstone mode:
- clAWS does **not** call `CreateAgentRuntime` — no new Gateway is created
- The `GatewayId` and `GatewayArn` outputs reference the shared Gateway
- Cedar policies in `ClawsPolicyStack` are still attached to the shared Gateway ARN
- All six tool Lambda endpoints are registered as before

## Retrieving the shared Gateway ID

If deploying alongside `quick-suite-router`:

```bash
aws cloudformation describe-stacks \
  --stack-name QuickSuiteRouterStack \
  --query 'Stacks[0].Outputs[?OutputKey==`GatewayId`].OutputValue' \
  --output text
```

Pass that value as `CLAWS_GATEWAY_ID` when deploying clAWS.

## Stack outputs

Both modes produce the same `ClawsGatewayStack` outputs:

| Output | Description |
|--------|-------------|
| `GatewayId` | AgentRuntime ID used for policy attachment |
| `GatewayArn` | Full ARN of the Gateway |
