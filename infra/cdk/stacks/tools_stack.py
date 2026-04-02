"""clAWS Tools Stack — Lambda handlers, IAM roles, Athena workgroups."""

import aws_cdk as cdk
from aws_cdk import (
    aws_athena as athena,
)
from aws_cdk import (
    aws_iam as iam,
)
from aws_cdk import (
    aws_lambda as _lambda,
)
from aws_cdk import (
    aws_ssm as ssm,
)
from constructs import Construct

TOOL_NAMES = ["discover", "probe", "plan", "excavate", "refine", "export", "watch", "watches"]


class ClawsToolsStack(cdk.Stack):
    def __init__(self, scope: Construct, id: str, storage_stack, guardrails_stack, **kwargs):
        super().__init__(scope, id, **kwargs)

        self.functions = {}

        # Read-only Athena workgroup with byte-scan limits
        self.athena_workgroup = athena.CfnWorkGroup(
            self, "ClawsReadOnlyWorkgroup",
            name="claws-readonly",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{storage_stack.athena_results_bucket.bucket_name}/",
                    encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(
                        encryption_option="SSE_S3",
                    ),
                ),
                bytes_scanned_cutoff_per_query=5_000_000_000,  # 5 GB limit
                enforce_work_group_configuration=True,
                publish_cloud_watch_metrics_enabled=True,
            ),
        )

        # Shared Lambda execution role
        lambda_role = iam.Role(
            self, "ClawsLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
            ],
        )

        # Grant access to storage
        storage_stack.runs_bucket.grant_read_write(lambda_role)
        storage_stack.athena_results_bucket.grant_read_write(lambda_role)
        storage_stack.plans_table.grant_read_write_data(lambda_role)
        storage_stack.schemas_table.grant_read_write_data(lambda_role)
        storage_stack.watches_table.grant_read_write_data(lambda_role)

        # Glue read-only for discover/probe
        lambda_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "glue:GetDatabases",
                "glue:GetDatabase",
                "glue:GetTables",
                "glue:GetTable",
            ],
            resources=["*"],
        ))

        # Athena read-only in the claws workgroup
        lambda_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "athena:StartQueryExecution",
                "athena:StopQueryExecution",
                "athena:GetQueryExecution",
                "athena:GetQueryResults",
            ],
            resources=["*"],
            conditions={
                "StringEquals": {
                    "athena:workGroup": "claws-readonly",
                },
            },
        ))

        # Bedrock access for plan and refine (model invocation + guardrails)
        lambda_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "bedrock:InvokeModel",
                "bedrock:ApplyGuardrail",
            ],
            resources=["*"],
        ))

        # CloudWatch metrics emission
        lambda_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["cloudwatch:PutMetricData"],
            resources=["*"],
            conditions={"StringEquals": {"cloudwatch:namespace": "claws"}},
        ))

        # EventBridge export destination
        lambda_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["events:PutEvents"],
            resources=["*"],
        ))

        # Quick Sight export destination (export tool)
        lambda_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "quicksight:CreateDataSource",
                "quicksight:CreateDataSet",
            ],
            resources=["*"],
        ))

        # Secrets Manager read for router OAuth credentials (plan + refine)
        lambda_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["secretsmanager:GetSecretValue"],
            resources=["*"],
            conditions={"StringLike": {"secretsmanager:SecretId": "*claws-router*"}},
        ))

        # Shared environment variables
        shared_env = {
            "CLAWS_RUNS_BUCKET": storage_stack.runs_bucket.bucket_name,
            "CLAWS_PLANS_TABLE": storage_stack.plans_table.table_name,
            "CLAWS_SCHEMAS_TABLE": storage_stack.schemas_table.table_name,
            "CLAWS_WATCHES_TABLE": storage_stack.watches_table.table_name,
            "CLAWS_ATHENA_WORKGROUP": "claws-readonly",
            "CLAWS_ATHENA_OUTPUT": f"s3://{storage_stack.athena_results_bucket.bucket_name}/",
            "CLAWS_GUARDRAIL_ID": guardrails_stack.base_guardrail_id,
            "CLAWS_GUARDRAIL_VERSION": "DRAFT",
            "CLAWS_METRICS_NAMESPACE": "claws",
        }

        # Create a Lambda for each tool
        for tool_name in TOOL_NAMES:
            fn = _lambda.Function(
                self, f"ClawsTool-{tool_name}",
                function_name=f"claws-{tool_name}",
                runtime=_lambda.Runtime.PYTHON_3_12,
                handler=f"tools.{tool_name}.handler.handler",
                code=_lambda.Code.from_asset("../../tools"),
                role=lambda_role,
                environment=shared_env,
                timeout=cdk.Duration.seconds(60),
                memory_size=512,
            )
            self.functions[tool_name] = fn

        # Per-tool extra env vars (added after the loop via add_environment)

        # export: Quick Sight destination + ClawsLookupTable
        qs_account_id = self.node.try_get_context("quicksight_account_id") or ""
        lookup_table = self.node.try_get_context("claws_lookup_table") or ""
        if "export" in self.functions:
            self.functions["export"].add_environment("QUICKSIGHT_ACCOUNT_ID", qs_account_id)
            self.functions["export"].add_environment("CLAWS_LOOKUP_TABLE", lookup_table)

        # plan + refine: Quick Suite model router
        router_endpoint = self.node.try_get_context("router_endpoint") or ""
        router_token_url = self.node.try_get_context("router_token_url") or ""
        router_secret_arn = self.node.try_get_context("router_secret_arn") or ""
        for tool_name in ("plan", "refine"):
            if tool_name in self.functions:
                self.functions[tool_name].add_environment("ROUTER_ENDPOINT", router_endpoint)
                self.functions[tool_name].add_environment("ROUTER_TOKEN_URL", router_token_url)
                self.functions[tool_name].add_environment("ROUTER_SECRET_ARN", router_secret_arn)

        # SSM export for qs-discover unified discovery Lambda
        if "discover" in self.functions:
            ssm.StringParameter(
                self,
                "ClawsDiscoverArnParam",
                parameter_name="/quick-suite/lambdas/claws-discover-arn",
                string_value=self.functions["discover"].function_arn,
                description="claws-discover Lambda ARN for qs-discover fan-out",
            )

        # Outputs
        for name, fn in self.functions.items():
            cdk.CfnOutput(self, f"{name}FunctionArn", value=fn.function_arn)
