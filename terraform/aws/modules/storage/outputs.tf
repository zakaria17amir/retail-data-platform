output "landing_bucket" {
  description = "Name of the landing bucket."
  value       = aws_s3_bucket.this["landing"].bucket
}

output "lakehouse_bucket" {
  description = "Name of the lakehouse bucket (Delta silver + Parquet export)."
  value       = aws_s3_bucket.this["lakehouse"].bucket
}

output "artifacts_bucket" {
  description = "Name of the artifacts bucket (job code, model artefacts)."
  value       = aws_s3_bucket.this["artifacts"].bucket
}

output "lakehouse_bucket_arn" {
  description = "ARN of the lakehouse bucket."
  value       = aws_s3_bucket.this["lakehouse"].arn
}

output "artifacts_bucket_arn" {
  description = "ARN of the artifacts bucket."
  value       = aws_s3_bucket.this["artifacts"].arn
}
