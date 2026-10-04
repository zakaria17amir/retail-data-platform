output "storage_aws_iam_user_arn" {
  description = "STORAGE_AWS_IAM_USER_ARN of the storage integration; the principal trusted by the AWS role (snowpipe-integration var snowflake_iam_user_arn)."
  value       = snowflake_storage_integration_aws.export.describe_output[0].iam_user_arn
}

output "storage_aws_external_id" {
  description = "STORAGE_AWS_EXTERNAL_ID of the storage integration; the sts:ExternalId condition of the AWS role (snowpipe-integration var snowflake_external_id)."
  value       = snowflake_storage_integration_aws.export.describe_output[0].external_id
}

output "pipe_notification_channels" {
  description = "notification_channel (Snowflake-managed SQS ARN) of each silver pipe, keyed by table."
  value       = { for k, p in snowflake_pipe.silver : k => p.notification_channel }
}

output "pipe_notification_channel" {
  description = "The single SQS ARN shared by all pipes on the stage; target of the S3 event notification on export/silver/."
  value       = one(distinct([for p in snowflake_pipe.silver : p.notification_channel]))
}

output "database" {
  description = "Snowflake database name (dbt SNOWFLAKE_DATABASE)."
  value       = snowflake_database.retail.name
}

output "warehouse" {
  description = "XS warehouse name (dbt SNOWFLAKE_WAREHOUSE)."
  value       = snowflake_warehouse.retail.name
}

output "dbt_user" {
  description = "dbt service user name (dbt SNOWFLAKE_USER); default role TRANSFORMER."
  value       = snowflake_service_user.dbt.name
}
