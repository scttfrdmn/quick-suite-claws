"""clAWS Policy Stack — Cedar policy deployment.

Note: As of this writing, AgentCore Cedar policy CDK constructs may be
in preview. This stack shows the intended configuration.
"""

import aws_cdk as cdk
from constructs import Construct


class ClawsPolicyStack(cdk.Stack):
    def __init__(self, scope: Construct, id: str, gateway_stack, **kwargs):
        super().__init__(scope, id, **kwargs)

        # TODO: AgentCore Policy CDK constructs
        #
        # When available, this stack will:
        # 1. Deploy Cedar policies from policies/default.cedar
        # 2. Associate policies with the AgentCore Gateway
        # 3. Support per-tenant policy overlays
        #
        # For now, Cedar policies are deployed via the AgentCore CLI:
        #   aws bedrock-agentcore create-policy \
        #     --policy-name claws-default \
        #     --policy-document file://policies/default.cedar

        cdk.CfnOutput(
            self, "PolicyNote",
            value="Deploy Cedar policies via: aws bedrock-agentcore create-policy --policy-document file://policies/default.cedar",
        )
