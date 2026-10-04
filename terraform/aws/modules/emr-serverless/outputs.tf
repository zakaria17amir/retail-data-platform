output "application_id" {
  description = "EMR Serverless application id (for start-job-run)."
  value       = aws_emrserverless_application.spark.id
}

output "application_arn" {
  description = "EMR Serverless application ARN (scopes the run role's StartJobRun)."
  value       = aws_emrserverless_application.spark.arn
}
