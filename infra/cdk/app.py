#!/usr/bin/env python3
"""clAWS CDK Application."""

import aws_cdk as cdk
from stacks.gateway_stack import ClawsGatewayStack
from stacks.guardrails_stack import ClawsGuardrailsStack
from stacks.policy_stack import ClawsPolicyStack
from stacks.scheduler_stack import ClawsSchedulerStack
from stacks.storage_stack import ClawsStorageStack
from stacks.tools_stack import ClawsToolsStack

app = cdk.App()

env = cdk.Environment(
    account=app.node.try_get_context("account") or None,
    region=app.node.try_get_context("region") or "us-east-1",
)

# Storage first — buckets, tables
storage = ClawsStorageStack(app, "ClawsStorageStack", env=env)

# Guardrails — Bedrock guardrail configs
guardrails = ClawsGuardrailsStack(app, "ClawsGuardrailsStack", env=env)

# Tool Lambdas — depend on storage and guardrails
tools = ClawsToolsStack(
    app, "ClawsToolsStack",
    storage_stack=storage,
    guardrails_stack=guardrails,
    env=env,
)

# Scheduler — watch runner Lambda + EventBridge Scheduler group
scheduler_stack = ClawsSchedulerStack(
    app, "ClawsSchedulerStack",
    storage_stack=storage,
    tools_stack=tools,
    env=env,
)

# AgentCore Gateway — depends on tools
# Pass CLAWS_GATEWAY_ID context var to reuse an existing Gateway (Capstone mode).
# Example: cdk deploy -c CLAWS_GATEWAY_ID=agr-abc123
shared_gateway_id = app.node.try_get_context("CLAWS_GATEWAY_ID") or None

gateway = ClawsGatewayStack(
    app, "ClawsGatewayStack",
    tools_stack=tools,
    shared_gateway_id=shared_gateway_id,
    env=env,
)

# Cedar policies — depends on gateway
policy = ClawsPolicyStack(
    app, "ClawsPolicyStack",
    gateway_stack=gateway,
    env=env,
)

app.synth()
