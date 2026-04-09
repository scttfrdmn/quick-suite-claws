"""clAWS Tools Stack — Lambda handlers, IAM roles, Athena workgroups."""

import os

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
    aws_logs as logs,
)
from aws_cdk import (
    aws_secretsmanager as secretsmanager,
)
from aws_cdk import (
    aws_ssm as ssm,
)
from constructs import Construct

# Resolve tools/ directory relative to this source file (infra/cdk/stacks/).
# Using __file__ avoids depending on the CDK process's working directory.
_TOOLS_DIR = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../tools")
)

# Lambda handlers live in tools/<name>/handler.py and import from tools.shared.
# The code asset bundles the tools/ directory into a tools/ subdirectory inside
# the Lambda zip so that "from tools.shared import ..." resolves correctly.
_LAMBDA_CODE = _lambda.Code.from_asset(
    _TOOLS_DIR,
    bundling=cdk.BundlingOptions(
        image=_lambda.Runtime.PYTHON_3_12.bundling_image,
        command=[
            "bash", "-c",
            "mkdir -p /asset-output/tools && cp -r /asset-input/. /asset-output/tools/",
        ],
    ),
)

TOOL_NAMES = [
    "discover", "probe", "plan", "excavate", "refine", "export",
    "watch", "watches",
    "team_plans", "share_plan",  # v0.10 collaboration tools
    "remember", "recall",  # v0.17 memory tools
]

# Internal Lambdas — not registered as AgentCore tools but deployed in the same stack
INTERNAL_LAMBDA_NAMES = ["approve_plan", "audit_export"]


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

        # Athena read-only in the claws workgroup — scoped to workgroup ARN.
        # Restricts Lambda to the 5 GB byte-scan-limit workgroup only.
        lambda_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "athena:StartQueryExecution",
                "athena:StopQueryExecution",
                "athena:GetQueryExecution",
                "athena:GetQueryResults",
                "athena:GetWorkGroup",
            ],
            resources=[
                f"arn:aws:athena:{self.region}:{self.account}:workgroup/claws-readonly",
            ],
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
                code=_LAMBDA_CODE,
                role=lambda_role,
                environment=shared_env,
                timeout=cdk.Duration.seconds(60),
                memory_size=512,
                log_retention=logs.RetentionDays.THREE_MONTHS,
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

        # Internal Lambdas — approve_plan and audit_export
        # These are NOT AgentCore tool targets. They are invoked directly
        # by IRB reviewers (approve_plan) and compliance pipelines (audit_export).
        irb_approvers = self.node.try_get_context("irb_approvers") or ""
        events_bus = self.node.try_get_context("events_bus") or "default"
        audit_log_group = self.node.try_get_context("audit_log_group") or "/aws/lambda/claws-audit"

        for internal_name in INTERNAL_LAMBDA_NAMES:
            fn = _lambda.Function(
                self, f"ClawsInternal-{internal_name}",
                function_name=f"claws-{internal_name.replace('_', '-')}",
                runtime=_lambda.Runtime.PYTHON_3_12,
                handler=f"tools.{internal_name}.handler.handler",
                code=_LAMBDA_CODE,
                role=lambda_role,
                environment={
                    **shared_env,
                    "CLAWS_IRB_APPROVERS": irb_approvers,
                    "CLAWS_EVENTS_BUS": events_bus,
                    "CLAWS_AUDIT_LOG_GROUP": audit_log_group,
                },
                timeout=cdk.Duration.seconds(300),  # audit_export may scan many log streams
                memory_size=512,
                log_retention=logs.RetentionDays.THREE_MONTHS,
            )
            self.functions[internal_name] = fn

        # audit_export: HMAC key secret for irreversible audit record hashing (issue #73)
        audit_hmac_secret = secretsmanager.Secret(
            self,
            "ClawsAuditHmacSecret",
            secret_name="claws/audit-hmac-key",
            description="HMAC-SHA-256 key for clAWS audit_export record hashing",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                password_length=64,
                exclude_punctuation=True,
            ),
        )
        if "audit_export" in self.functions:
            self.functions["audit_export"].add_environment(
                "AUDIT_HMAC_KEY_ARN", audit_hmac_secret.secret_arn
            )
            audit_hmac_secret.grant_read(self.functions["audit_export"])

        # audit_export: grant CloudWatch Logs read and S3 write to the export bucket
        if "audit_export" in self.functions:
            self.functions["audit_export"].add_to_role_policy(iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "logs:DescribeLogStreams",
                    "logs:FilterLogEvents",
                    "logs:GetLogEvents",
                ],
                resources=["*"],
            ))
            # S3 write permission for arbitrary output URIs
            self.functions["audit_export"].add_to_role_policy(iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["s3:PutObject"],
                resources=["*"],
            ))

        # approve_plan: grant EventBridge PutEvents for IRB approval events
        if "approve_plan" in self.functions:
            self.functions["approve_plan"].add_to_role_policy(iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["events:PutEvents"],
                resources=["*"],
            ))

        # v0.17.0 memory tools: inject env vars and grant IAM
        memory_registrar_arn = self.node.try_get_context("memory_registrar_arn") or ""
        if "remember" in self.functions:
            self.functions["remember"].add_environment(
                "CLAWS_MEMORY_BUCKET", storage_stack.memory_bucket.bucket_name
            )
            self.functions["remember"].add_environment(
                "CLAWS_MEMORY_REGISTRY_TABLE", storage_stack.memory_registry_table.table_name
            )
            if memory_registrar_arn:
                self.functions["remember"].add_environment(
                    "MEMORY_REGISTRAR_ARN", memory_registrar_arn
                )
            storage_stack.memory_bucket.grant_read_write(self.functions["remember"])
            storage_stack.memory_registry_table.grant_read_write_data(self.functions["remember"])
            self.functions["remember"].add_to_role_policy(iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["lambda:InvokeFunction"],
                resources=["*"],
                conditions={"StringLike": {"lambda:FunctionArn": "*registrar*"}},
            ))

        if "recall" in self.functions:
            self.functions["recall"].add_environment(
                "CLAWS_MEMORY_BUCKET", storage_stack.memory_bucket.bucket_name
            )
            storage_stack.memory_bucket.grant_read(self.functions["recall"])

        # v0.18.0 — PostgreSQL executor (#63)
        postgres_secret_arn = self.node.try_get_context("postgres_secret_arn") or ""
        if postgres_secret_arn and "excavate" in self.functions:
            self.functions["excavate"].add_environment(
                "CLAWS_POSTGRES_SECRET_ARN", postgres_secret_arn
            )
            self.functions["excavate"].add_to_role_policy(iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["secretsmanager:GetSecretValue"],
                resources=[postgres_secret_arn],
            ))

        # v0.18.0 — Redshift executor (#64)
        redshift_workgroup = self.node.try_get_context("redshift_workgroup") or ""
        redshift_database = self.node.try_get_context("redshift_database") or ""
        if "excavate" in self.functions:
            self.functions["excavate"].add_environment(
                "CLAWS_REDSHIFT_WORKGROUP", redshift_workgroup
            )
            self.functions["excavate"].add_environment(
                "CLAWS_REDSHIFT_DATABASE", redshift_database
            )
            if redshift_workgroup:
                self.functions["excavate"].add_to_role_policy(iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "redshift-data:ExecuteStatement",
                        "redshift-data:DescribeStatement",
                        "redshift-data:GetStatementResult",
                    ],
                    resources=["*"],
                ))

        # v0.18.0 — Per-principal budget caps (#65)
        enable_principal_budgets = self.node.try_get_context("enable_principal_budgets") or ""
        for tool_name in TOOL_NAMES:
            if tool_name in self.functions:
                self.functions[tool_name].add_environment(
                    "CLAWS_PRINCIPAL_SPEND_TABLE",
                    storage_stack.principal_spend_table.table_name,
                )
        if enable_principal_budgets:
            for tool_name in TOOL_NAMES:
                if tool_name in self.functions:
                    self.functions[tool_name].add_environment(
                        "CLAWS_ENABLE_PRINCIPAL_BUDGETS", enable_principal_budgets
                    )
        storage_stack.principal_spend_table.grant_read_write_data(lambda_role)
        lambda_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["ssm:GetParameter"],
            resources=[
                f"arn:aws:ssm:{self.region}:{self.account}:parameter/quick-suite/claws/budget/*",
            ],
        ))

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
