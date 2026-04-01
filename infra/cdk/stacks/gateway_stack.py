"""clAWS Gateway Stack — AgentCore Gateway and tool endpoint registration.

Uses AwsCustomResource to call AgentCore control-plane APIs since L2 CDK
constructs are not yet available for AgentCore.
"""

import aws_cdk as cdk
from aws_cdk import (
    aws_iam as iam,
)
from aws_cdk import (
    aws_lambda as _lambda,
)
from aws_cdk import (
    custom_resources as cr,
)
from constructs import Construct

TOOL_NAMES = ["discover", "probe", "plan", "excavate", "refine", "export"]


class ClawsGatewayStack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        tools_stack,
        shared_gateway_id: str | None = None,
        **kwargs,
    ):
        super().__init__(scope, id, **kwargs)

        # IAM role for the AwsCustomResource provider Lambda
        provider_role = iam.Role(
            self, "AgentCoreProviderRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
            ],
        )
        provider_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "bedrock-agentcore:CreateAgentRuntime",
                "bedrock-agentcore:DeleteAgentRuntime",
                "bedrock-agentcore:GetAgentRuntime",
                "bedrock-agentcore:CreateAgentRuntimeEndpoint",
                "bedrock-agentcore:DeleteAgentRuntimeEndpoint",
            ],
            resources=["*"],
        ))

        if shared_gateway_id:
            # Capstone mode: attach to an existing Gateway rather than creating one
            self.gateway_id = shared_gateway_id
            self.gateway_arn = (
                f"arn:aws:bedrock-agentcore:{self.region}:{self.account}"
                f":agent-runtime/{shared_gateway_id}"
            )
        else:
            # Standalone mode: create a new AgentCore Gateway (AgentRuntime)
            gateway_resource = cr.AwsCustomResource(
                self, "ClawsGateway",
                on_create=cr.AwsSdkCall(
                    service="BedrockAgentCore",
                    action="createAgentRuntime",
                    parameters={
                        "agentRuntimeName": "claws-gateway",
                        "description": "clAWS secure data excavation tool plane",
                    },
                    physical_resource_id=cr.PhysicalResourceId.from_response("agentRuntimeId"),
                ),
                on_delete=cr.AwsSdkCall(
                    service="BedrockAgentCore",
                    action="deleteAgentRuntime",
                    parameters={
                        "agentRuntimeId": cr.PhysicalResourceIdReference(),
                    },
                ),
                role=provider_role,
            )

            self.gateway_id = gateway_resource.get_response_field("agentRuntimeId")
            self.gateway_arn = gateway_resource.get_response_field("agentRuntimeArn")

        # Register each tool Lambda as a Gateway endpoint
        for tool_name in TOOL_NAMES:
            fn: _lambda.IFunction = tools_stack.functions[tool_name]

            # Allow AgentCore to invoke this Lambda
            fn.add_permission(
                f"AllowAgentCore-{tool_name}",
                principal=iam.ServicePrincipal("bedrock-agentcore.amazonaws.com"),
                action="lambda:InvokeFunction",
                source_arn=self.gateway_arn,
            )

            # Register tool as a Gateway endpoint
            cr.AwsCustomResource(
                self, f"ClawsEndpoint-{tool_name}",
                on_create=cr.AwsSdkCall(
                    service="BedrockAgentCore",
                    action="createAgentRuntimeEndpoint",
                    parameters={
                        "agentRuntimeId": self.gateway_id,
                        "name": f"claws-{tool_name}",
                        "lambdaArn": fn.function_arn,
                    },
                    physical_resource_id=cr.PhysicalResourceId.from_response("endpointId"),
                ),
                on_delete=cr.AwsSdkCall(
                    service="BedrockAgentCore",
                    action="deleteAgentRuntimeEndpoint",
                    parameters={
                        "agentRuntimeId": self.gateway_id,
                        "endpointId": cr.PhysicalResourceIdReference(),
                    },
                ),
                role=provider_role,
            )

        cdk.CfnOutput(self, "GatewayId", value=self.gateway_id)
        cdk.CfnOutput(self, "GatewayArn", value=self.gateway_arn)
