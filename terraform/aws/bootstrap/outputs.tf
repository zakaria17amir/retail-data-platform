output "state_bucket" {
  description = "Name of the Terraform remote state bucket."
  value       = aws_s3_bucket.state.bucket
}

output "lock_table" {
  description = "Name of the DynamoDB state lock table."
  value       = aws_dynamodb_table.lock.name
}
