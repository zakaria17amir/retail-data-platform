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
