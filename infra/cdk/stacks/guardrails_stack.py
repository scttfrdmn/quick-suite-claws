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

        # FERPA guardrail — deployed when 'enable_ferpa_guardrail' CDK context is true.
        # When enabled, the FERPA guardrail replaces the base guardrail on workloads
        # that process student education records. Set via:
        #   cdk deploy --context enable_ferpa_guardrail=true
        # or in cdk.json:
        #   { "context": { "enable_ferpa_guardrail": true } }
        enable_ferpa = self.node.try_get_context("enable_ferpa_guardrail") or False
        if enable_ferpa:
            self.ferpa_guardrail = bedrock.CfnGuardrail(
                self, "ClawsFerpaGuardrail",
                name="claws-ferpa",
                description=(
                    "FERPA-compliant guardrail for clAWS — blocks access to FERPA-protected "
                    "student education record categories and detects student ID / SSN patterns."
                ),
                blocked_input_messaging=(
                    "This request involves FERPA-protected student data categories "
                    "that cannot be accessed through this system."
                ),
                blocked_outputs_messaging=(
                    "The result contains FERPA-protected student data that cannot be returned."
                ),

                # Deny FERPA-protected topic categories
                topic_policy_config=bedrock.CfnGuardrail.TopicPolicyConfigProperty(
                    topics_config=[
                        bedrock.CfnGuardrail.TopicConfigProperty(
                            name="grades",
                            definition=(
                                "Student academic grades, GPA, transcripts, or academic "
                                "performance records"
                            ),
                            type="DENY",
                        ),
                        bedrock.CfnGuardrail.TopicConfigProperty(
                            name="enrollment_status",
                            definition=(
                                "Whether a student is currently enrolled, their enrollment "
                                "history, or registration details"
                            ),
                            type="DENY",
                        ),
                        bedrock.CfnGuardrail.TopicConfigProperty(
                            name="financial_aid",
                            definition=(
                                "Student financial aid awards, loans, grants, scholarships, "
                                "or financial need assessments"
                            ),
                            type="DENY",
                        ),
                        bedrock.CfnGuardrail.TopicConfigProperty(
                            name="disciplinary_records",
                            definition=(
                                "Student disciplinary history, conduct violations, sanctions, "
                                "or academic integrity records"
                            ),
                            type="DENY",
                        ),
                        bedrock.CfnGuardrail.TopicConfigProperty(
                            name="student_schedules",
                            definition=(
                                "Individual student class schedules or course registrations "
                                "that identify a specific student"
                            ),
                            type="DENY",
                        ),
                    ],
                ),

                # PII + student-specific regex patterns
                sensitive_information_policy_config=bedrock.CfnGuardrail.SensitiveInformationPolicyConfigProperty(
                    pii_entities_config=[
                        bedrock.CfnGuardrail.PiiEntityConfigProperty(
                            type="US_SOCIAL_SECURITY_NUMBER", action="BLOCK",
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
                            type="ADDRESS", action="ANONYMIZE",
                        ),
                        bedrock.CfnGuardrail.PiiEntityConfigProperty(
                            type="DATE_TIME", action="ANONYMIZE",
                        ),
                    ],
                    regexes_config=[
                        bedrock.CfnGuardrail.RegexConfigProperty(
                            name="ssn_pattern",
                            description="US Social Security Number pattern",
                            pattern=r"\d{3}-\d{2}-\d{4}",
                            action="BLOCK",
                        ),
                        bedrock.CfnGuardrail.RegexConfigProperty(
                            name="student_id_pattern",
                            description="Student ID pattern (letter followed by 7 digits)",
                            pattern=r"[A-Z]\d{7}",
                            action="BLOCK",
                        ),
                    ],
                ),

                # Prompt attack filter
                content_policy_config=bedrock.CfnGuardrail.ContentPolicyConfigProperty(
                    filters_config=[
                        bedrock.CfnGuardrail.ContentFilterConfigProperty(
                            type="PROMPT_ATTACK",
                            input_strength="HIGH",
                            output_strength="NONE",
                        ),
                    ],
                ),
            )
            self.ferpa_guardrail_id = self.ferpa_guardrail.attr_guardrail_id
            cdk.CfnOutput(self, "FerpaGuardrailId",
                           value=self.ferpa_guardrail.attr_guardrail_id)
            cdk.CfnOutput(self, "FerpaGuardrailArn",
                           value=self.ferpa_guardrail.attr_guardrail_arn)
        else:
            self.ferpa_guardrail_id = None

        # Export guardrail ID for other stacks
        self.base_guardrail_id = self.base_guardrail.attr_guardrail_id

        cdk.CfnOutput(self, "BaseGuardrailId",
                       value=self.base_guardrail.attr_guardrail_id)
        cdk.CfnOutput(self, "BaseGuardrailArn",
                       value=self.base_guardrail.attr_guardrail_arn)
