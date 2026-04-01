"""clAWS Storage Stack — S3 buckets, DynamoDB tables."""

import aws_cdk as cdk
from aws_cdk import (
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    RemovalPolicy,
)
from constructs import Construct


class ClawsStorageStack(cdk.Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # S3 bucket for excavation run results
        self.runs_bucket = s3.Bucket(
            self, "RunsBucket",
            bucket_name=f"claws-runs-{cdk.Aws.ACCOUNT_ID}",
            removal_policy=RemovalPolicy.RETAIN,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="expire-old-runs",
                    expiration=cdk.Duration.days(30),
                    noncurrent_version_expiration=cdk.Duration.days(7),
                ),
            ],
            versioned=True,
        )

        # S3 bucket for Athena query results
        self.athena_results_bucket = s3.Bucket(
            self, "AthenaResultsBucket",
            bucket_name=f"claws-athena-results-{cdk.Aws.ACCOUNT_ID}",
            removal_policy=RemovalPolicy.RETAIN,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="expire-athena-results",
                    expiration=cdk.Duration.days(7),
                ),
            ],
        )

        # DynamoDB table for execution plans (TTL-enabled)
        self.plans_table = dynamodb.Table(
            self, "PlansTable",
            table_name="claws-plans",
            partition_key=dynamodb.Attribute(
                name="plan_id",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            time_to_live_attribute="ttl",
            point_in_time_recovery=True,
        )

        # DynamoDB table for cached schemas (TTL-enabled)
        self.schemas_table = dynamodb.Table(
            self, "SchemasTable",
            table_name="claws-schemas",
            partition_key=dynamodb.Attribute(
                name="source_id",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            time_to_live_attribute="ttl",
        )

        # Outputs
        cdk.CfnOutput(self, "RunsBucketName", value=self.runs_bucket.bucket_name)
        cdk.CfnOutput(self, "PlansTableName", value=self.plans_table.table_name)
        cdk.CfnOutput(self, "SchemasTableName", value=self.schemas_table.table_name)
