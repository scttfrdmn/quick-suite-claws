"""clAWS Guardrails Stack — Bedrock Guardrail configurations."""


import aws_cdk as cdk
from aws_cdk import aws_bedrock as bedrock
from constructs import Construct


class ClawsGuardrailsStack(cdk.Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # Base guardrail — shared across all tenants
        self.base_guardrail = bedrock.CfnGuardrail(
            self, "ClawsBaseGuardrail",
            name="claws-base",
            description="clAWS base content safety guardrail",
            blocked_input_messaging="Request blocked by clAWS content policy.",
            blocked_outputs_messaging="Response blocked by clAWS content policy.",

            # Content filters — block harmful content
            content_policy_config=bedrock.CfnGuardrail.ContentPolicyConfigProperty(
                filters_config=[
                    bedrock.CfnGuardrail.ContentFilterConfigProperty(
                        type="SEXUAL",
                        input_strength="HIGH",
                        output_strength="HIGH",
                    ),
                    bedrock.CfnGuardrail.ContentFilterConfigProperty(
                        type="VIOLENCE",
                        input_strength="HIGH",
                        output_strength="HIGH",
                    ),
                    bedrock.CfnGuardrail.ContentFilterConfigProperty(
                        type="HATE",
                        input_strength="HIGH",
                        output_strength="HIGH",
                    ),
                    bedrock.CfnGuardrail.ContentFilterConfigProperty(
                        type="INSULTS",
                        input_strength="MEDIUM",
                        output_strength="MEDIUM",
                    ),
                    bedrock.CfnGuardrail.ContentFilterConfigProperty(
                        type="MISCONDUCT",
                        input_strength="HIGH",
                        output_strength="HIGH",
                    ),
                    bedrock.CfnGuardrail.ContentFilterConfigProperty(
                        type="PROMPT_ATTACK",
                        input_strength="HIGH",
                        output_strength="NONE",
                    ),
                ],
            ),

            # Sensitive information — PII/PHI detection and masking
            sensitive_information_policy_config=bedrock.CfnGuardrail.SensitiveInformationPolicyConfigProperty(
                pii_entities_config=[
                    bedrock.CfnGuardrail.PiiEntityConfigProperty(
                        type="US_SOCIAL_SECURITY_NUMBER", action="BLOCK",
                    ),
                    bedrock.CfnGuardrail.PiiEntityConfigProperty(
                        type="CREDIT_DEBIT_CARD_NUMBER", action="BLOCK",
                    ),
                    bedrock.CfnGuardrail.PiiEntityConfigProperty(
                        type="CREDIT_DEBIT_CARD_CVV", action="BLOCK",
                    ),
                    bedrock.CfnGuardrail.PiiEntityConfigProperty(
                        type="CREDIT_DEBIT_CARD_EXPIRY", action="BLOCK",
                    ),
                    bedrock.CfnGuardrail.PiiEntityConfigProperty(
                        type="PIN", action="BLOCK",
                    ),
                    bedrock.CfnGuardrail.PiiEntityConfigProperty(
                        type="PASSWORD", action="BLOCK",
                    ),
                    bedrock.CfnGuardrail.PiiEntityConfigProperty(
                        type="AWS_ACCESS_KEY", action="BLOCK",
                    ),
                    bedrock.CfnGuardrail.PiiEntityConfigProperty(
                        type="AWS_SECRET_KEY", action="BLOCK",
                    ),
                    bedrock.CfnGuardrail.PiiEntityConfigProperty(
                        type="EMAIL", action="ANONYMIZE",
                    ),
                    bedrock.CfnGuardrail.PiiEntityConfigProperty(
                        type="PHONE", action="ANONYMIZE",
                    ),
                    bedrock.CfnGuardrail.PiiEntityConfigProperty(
                        type="NAME", action="ANONYMIZE",
                    ),
                    bedrock.CfnGuardrail.PiiEntityConfigProperty(
                        type="US_INDIVIDUAL_TAX_IDENTIFICATION_NUMBER", action="BLOCK",
                    ),
                ],
                regexes_config=[
                    bedrock.CfnGuardrail.RegexConfigProperty(
                        name="internal_project_code",
                        description="Internal project identifiers",
                        pattern=r"PRJ-\d{6}",
                        action="ANONYMIZE",
                    ),
                    bedrock.CfnGuardrail.RegexConfigProperty(
                        name="medical_record_number",
                        description="Medical record numbers (common formats)",
                        pattern=r"MRN[-:]?\s?\d{6,10}",
                        action="BLOCK",
                    ),
                ],
            ),

            # Word filters — catch-all for known sensitive terms
            word_policy_config=bedrock.CfnGuardrail.WordPolicyConfigProperty(
                words_config=[
                    bedrock.CfnGuardrail.WordConfigProperty(text="CONFIDENTIAL-INTERNAL"),
                    bedrock.CfnGuardrail.WordConfigProperty(text="TOP-SECRET"),
                ],
            ),
        )

        # Export guardrail ID for other stacks
        self.base_guardrail_id = self.base_guardrail.attr_guardrail_id

        cdk.CfnOutput(self, "BaseGuardrailId",
                       value=self.base_guardrail.attr_guardrail_id)
        cdk.CfnOutput(self, "BaseGuardrailArn",
                       value=self.base_guardrail.attr_guardrail_arn)
