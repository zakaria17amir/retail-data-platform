output "landing_bucket" {
  description = "Name of the landing bucket."
  value       = module.storage.landing_bucket
}

output "lakehouse_bucket" {
  description = "Name of the lakehouse bucket."
  value       = module.storage.lakehouse_bucket
}

output "artifacts_bucket" {
  description = "Name of the artifacts bucket."
  value       = module.storage.artifacts_bucket
}

output "github_deploy_role_arn" {
  description = "Role ARN for the Terraform workflow (AWS_PLAN_ROLE_ARN)."
  value       = module.iam.github_deploy_role_arn
}

output "github_run_role_arn" {
  description = "Role ARN for the cloud batch workflow."
  value       = module.iam.github_run_role_arn
}

output "emr_job_role_arn" {
  description = "EMR Serverless job runtime role ARN."
  value       = module.iam.emr_job_role_arn
}

output "emr_log_group_name" {
  description = "CloudWatch log group for EMR Serverless job runs."
  value       = module.observability.emr_log_group_name
}

output "serving_log_group_name" {
  description = "CloudWatch log group for the ECS serving task."
  value       = module.observability.serving_log_group_name
}

output "emr_application_id" {
  description = "EMR Serverless application id for start-job-run."
  value       = module.emr_serverless.application_id
}

output "ecr_repository_url" {
  description = "ECR repository for the serving image."
  value       = module.serving.ecr_repository_url
}

output "serving_cluster_name" {
  description = "ECS cluster running the serving service."
  value       = module.serving.cluster_name
}

output "serving_service_name" {
  description = "ECS serving service (scale with aws ecs update-service --desired-count); null unless enable_serving."
  value       = module.serving.service_name
}

output "serving_url" {
  description = "Serving base URL (ALB, allowed CIDR only); null unless enable_serving."
  value       = var.enable_serving ? "http://${module.serving.alb_dns_name}" : null
}

output "serving_config_secret_arn" {
  description = "Secret holding MLFLOW_TRACKING_URI for the serving task; set before scaling up."
  value       = module.serving.config_secret_arn
}

output "snowflake_role_arn" {
  description = "Role ARN for the Snowflake storage integration (STORAGE_AWS_ROLE_ARN)."
  value       = module.snowpipe_integration.snowflake_role_arn
}

output "snowpipe_sqs_queue_arn" {
  description = "Snowflake-managed SQS queue notified on new export Parquet files."
  value       = module.snowpipe_integration.sqs_queue_arn
}
