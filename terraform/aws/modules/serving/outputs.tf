output "ecr_repository_url" {
  description = "ECR repository URL CI pushes the serving image to."
  value       = aws_ecr_repository.serving.repository_url
}

output "ecr_repository_arn" {
  description = "ECR repository ARN (scopes the run role's image push)."
  value       = aws_ecr_repository.serving.arn
}

output "cluster_name" {
  description = "ECS cluster name."
  value       = aws_ecs_cluster.serving.name
}

output "service_name" {
  description = "ECS service name (scale with update-service --desired-count)."
  value       = aws_ecs_service.serving.name
}

output "alb_dns_name" {
  description = "Public DNS name of the serving ALB (HTTP, allowed CIDR only)."
  value       = aws_lb.serving.dns_name
}

output "config_secret_arn" {
  description = "Secrets Manager secret holding MLFLOW_TRACKING_URI; set its value before scaling up."
  value       = aws_secretsmanager_secret.config.arn
}
