output "snowflake_role_arn" {
  description = "ARN of the role Snowflake's storage integration assumes. Known before the role exists (deterministic name), so step 1 of the two-step apply can hand it to the Snowflake integration."
  value       = "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:role/${local.role_name}"
}

output "sqs_queue_arn" {
  description = "Snowflake-managed SQS queue the export prefix notifies; null until snowflake_sqs_arn is set."
  value       = var.snowflake_sqs_arn != "" ? var.snowflake_sqs_arn : null
}
