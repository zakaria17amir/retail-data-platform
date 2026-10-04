output "alerts_topic_arn" {
  description = "ARN of the SNS topic that receives alarm notifications."
  value       = aws_sns_topic.alerts.arn
}

output "emr_log_group_name" {
  description = "CloudWatch log group for EMR Serverless job runs."
  value       = aws_cloudwatch_log_group.this["emr-serverless"].name
}

output "emr_log_group_arn" {
  description = "ARN of the EMR Serverless log group."
  value       = aws_cloudwatch_log_group.this["emr-serverless"].arn
}

output "serving_log_group_name" {
  description = "CloudWatch log group for the ECS serving task."
  value       = aws_cloudwatch_log_group.this["serving"].name
}
