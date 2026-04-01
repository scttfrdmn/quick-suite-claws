#!/usr/bin/env python3
"""clAWS CDK Application."""

import aws_cdk as cdk
from stacks.gateway_stack import ClawsGatewayStack
from stacks.guardrails_stack import ClawsGuardrailsStack
from stacks.policy_stack import ClawsPolicyStack
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

# AgentCore Gateway — depends on tools
gateway = ClawsGatewayStack(
    app, "ClawsGatewayStack",
    tools_stack=tools,
    env=env,
)

# Cedar policies — depends on gateway
policy = ClawsPolicyStack(
    app, "ClawsPolicyStack",
    gateway_stack=gateway,
    env=env,
)

app.synth()
