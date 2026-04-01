"""clAWS Gateway Stack — AgentCore Gateway tool registration.

Note: As of this writing, AgentCore Gateway CDK constructs may be
in preview. This stack shows the intended configuration; the actual
deployment may use custom resources or CLI-based setup.
"""

import aws_cdk as cdk
from constructs import Construct


class ClawsGatewayStack(cdk.Stack):
    def __init__(self, scope: Construct, id: str, tools_stack, **kwargs):
        super().__init__(scope, id, **kwargs)

        self.tools_stack = tools_stack

        # TODO: AgentCore Gateway CDK constructs
        #
        # When available, this stack will:
        # 1. Create an AgentCore Gateway
        # 2. Register each clAWS tool Lambda as a Gateway tool
        # 3. Configure OpenAPI spec from api/openapi.yaml
        # 4. Associate Cedar policies from the policy stack
        #
        # For now, registration is done via gateway-config.yaml
        # and the AgentCore CLI.

        # Placeholder outputs for the gateway endpoint
        cdk.CfnOutput(
            self, "GatewayNote",
            value="Configure AgentCore Gateway via infra/agentcore/gateway-config.yaml",
        )
