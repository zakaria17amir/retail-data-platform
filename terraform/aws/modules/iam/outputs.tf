output "github_deploy_role_arn" {
  description = "ARN of the GitHub OIDC role that runs Terraform."
  value       = aws_iam_role.github_deploy.arn
}

output "github_run_role_arn" {
  description = "ARN of the GitHub OIDC role that runs the cloud batch."
  value       = aws_iam_role.github_run.arn
}

output "emr_job_role_arn" {
  description = "ARN of the EMR Serverless job runtime role."
  value       = aws_iam_role.emr_job.arn
}
