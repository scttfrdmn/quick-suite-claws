"""clAWS Scheduler Stack — watch runner Lambda and EventBridge Scheduler group."""

import os

import aws_cdk as cdk
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_scheduler as scheduler
from constructs import Construct

_TOOLS_DIR = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../tools")
)


class ClawsSchedulerStack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        storage_stack,
        tools_stack,
        **kwargs,
    ):
        super().__init__(scope, id, **kwargs)

        # IAM role for the watch runner Lambda
        runner_role = iam.Role(
            self, "WatchRunnerRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
            ],
        )

        # Storage permissions
        storage_stack.watches_table.grant_read_write_data(runner_role)
        storage_stack.plans_table.grant_read_data(runner_role)
        storage_stack.runs_bucket.grant_read_write(runner_role)
        storage_stack.athena_results_bucket.grant_read_write(runner_role)

        # Athena execution (read-only workgroup)
        runner_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "athena:StartQueryExecution",
                "athena:StopQueryExecution",
                "athena:GetQueryExecution",
                "athena:GetQueryResults",
            ],
            resources=["*"],
            conditions={"StringEquals": {"athena:workGroup": "claws-readonly"}},
        ))

        # DynamoDB PartiQL (for dynamodb_partiql query types)
        runner_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["dynamodb:PartiQLSelect"],
            resources=["*"],
        ))

        # EventBridge export from notification targets
        runner_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["events:PutEvents"],
            resources=["*"],
        ))

        # SSM Parameter Store read — new_award watch fetches lab profile from SSM
        runner_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["ssm:GetParameter"],
            resources=[f"arn:aws:ssm:{cdk.Stack.of(self).region}:{cdk.Stack.of(self).account}:parameter/quick-suite/claws/*"],
        ))

        # IAM role for EventBridge Scheduler to invoke the runner Lambda
        scheduler_invoke_role = iam.Role(
            self, "SchedulerInvokeRole",
            assumed_by=iam.ServicePrincipal("scheduler.amazonaws.com"),
        )

        # Watch runner Lambda
        self.runner_fn = _lambda.Function(
            self, "WatchRunner",
            function_name="claws-watch-runner",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="tools.watch.runner.handler",
            code=_lambda.Code.from_asset(
                _TOOLS_DIR,
                bundling=cdk.BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_12.bundling_image,
                    command=[
                        "bash", "-c",
                        "mkdir -p /asset-output/tools && cp -r /asset-input/. /asset-output/tools/",
                    ],
                ),
            ),
            role=runner_role,
            environment={
                "CLAWS_RUNS_BUCKET": storage_stack.runs_bucket.bucket_name,
                "CLAWS_PLANS_TABLE": storage_stack.plans_table.table_name,
                "CLAWS_WATCHES_TABLE": storage_stack.watches_table.table_name,
                "CLAWS_ATHENA_WORKGROUP": "claws-readonly",
                "CLAWS_ATHENA_OUTPUT": (
                    f"s3://{storage_stack.athena_results_bucket.bucket_name}/"
                ),
            },
            timeout=cdk.Duration.minutes(5),
            memory_size=512,
        )

        # Grant scheduler role permission to invoke the runner Lambda
        self.runner_fn.grant_invoke(scheduler_invoke_role)

        # EventBridge Scheduler group for all watch schedules
        self.schedule_group = scheduler.CfnScheduleGroup(
            self, "ClawsWatchesGroup",
            name="claws-watches",
        )

        # Grant the watch tool Lambda permission to manage schedules in this group
        if "watch" in tools_stack.functions:
            tools_stack.functions["watch"].add_to_role_policy(iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "scheduler:CreateSchedule",
                    "scheduler:UpdateSchedule",
                    "scheduler:DeleteSchedule",
                    "scheduler:GetSchedule",
                ],
                resources=[
                    f"arn:aws:scheduler:{self.region}:{self.account}:"
                    f"schedule/claws-watches/*"
                ],
            ))
            # Also allow passing the scheduler invoke role to EventBridge
            tools_stack.functions["watch"].add_to_role_policy(iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["iam:PassRole"],
                resources=[scheduler_invoke_role.role_arn],
            ))
            # Inject runner ARN and role ARN into watch tool env
            tools_stack.functions["watch"].add_environment(
                "CLAWS_WATCH_RUNNER_ARN", self.runner_fn.function_arn
            )
            tools_stack.functions["watch"].add_environment(
                "CLAWS_WATCH_RUNNER_ROLE_ARN", scheduler_invoke_role.role_arn
            )
            tools_stack.functions["watch"].add_environment(
                "CLAWS_WATCHES_TABLE", storage_stack.watches_table.table_name
            )

        # Outputs
        cdk.CfnOutput(self, "WatchRunnerArn", value=self.runner_fn.function_arn)
        cdk.CfnOutput(self, "SchedulerInvokeRoleArn", value=scheduler_invoke_role.role_arn)
        cdk.CfnOutput(self, "ScheduleGroupName", value=self.schedule_group.name)
