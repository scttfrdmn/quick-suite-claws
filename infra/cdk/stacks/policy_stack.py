"""clAWS Policy Stack — Cedar policy deployment and gateway association.

Uses AwsCustomResource to call AgentCore control-plane APIs since L2 CDK
constructs are not yet available for AgentCore.
"""

from pathlib import Path

import aws_cdk as cdk
from aws_cdk import (
    aws_iam as iam,
)
from aws_cdk import (
    custom_resources as cr,
)
from constructs import Construct

# Cedar policy document read at synth time
_CEDAR_POLICY_PATH = Path(__file__).parents[3] / "policies" / "default.cedar"


class ClawsPolicyStack(cdk.Stack):
    def __init__(self, scope: Construct, id: str, gateway_stack, **kwargs):
        super().__init__(scope, id, **kwargs)

        cedar_document = _CEDAR_POLICY_PATH.read_text()

        # IAM role for the AwsCustomResource provider Lambda
        provider_role = iam.Role(
            self, "PolicyProviderRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
            ],
        )
        provider_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "bedrock-agentcore:CreatePolicy",
                "bedrock-agentcore:DeletePolicy",
                "bedrock-agentcore:AssociateAgentRuntimeWithPolicy",
                "bedrock-agentcore:DisassociateAgentRuntimeFromPolicy",
            ],
            resources=["*"],
        ))

        # Create the Cedar policy
        policy_resource = cr.AwsCustomResource(
            self, "ClawsCedarPolicy",
            on_create=cr.AwsSdkCall(
                service="BedrockAgentCore",
                action="createPolicy",
                parameters={
                    "policyName": "claws-default",
                    "policyType": "CEDAR",
                    "policyDocument": cedar_document,
                    "description": "clAWS default Cedar authorization policy",
                },
                physical_resource_id=cr.PhysicalResourceId.from_response("policyId"),
            ),
            on_update=cr.AwsSdkCall(
                service="BedrockAgentCore",
                action="createPolicy",
                parameters={
                    "policyName": "claws-default",
                    "policyType": "CEDAR",
                    "policyDocument": cedar_document,
                    "description": "clAWS default Cedar authorization policy",
                },
                physical_resource_id=cr.PhysicalResourceId.from_response("policyId"),
            ),
            on_delete=cr.AwsSdkCall(
                service="BedrockAgentCore",
                action="deletePolicy",
                parameters={
                    "policyId": cr.PhysicalResourceIdReference(),
                },
            ),
            role=provider_role,
        )

        self.policy_id = policy_resource.get_response_field("policyId")

        # Associate Cedar policy with the AgentCore Gateway
        cr.AwsCustomResource(
            self, "ClawsPolicyAssociation",
            on_create=cr.AwsSdkCall(
                service="BedrockAgentCore",
                action="associateAgentRuntimeWithPolicy",
                parameters={
                    "agentRuntimeId": gateway_stack.gateway_id,
                    "policyId": self.policy_id,
                },
                physical_resource_id=cr.PhysicalResourceId.of("claws-policy-association"),
            ),
            on_delete=cr.AwsSdkCall(
                service="BedrockAgentCore",
                action="disassociateAgentRuntimeFromPolicy",
                parameters={
                    "agentRuntimeId": gateway_stack.gateway_id,
                    "policyId": self.policy_id,
                },
            ),
            role=provider_role,
        )

        cdk.CfnOutput(self, "PolicyId", value=self.policy_id)
