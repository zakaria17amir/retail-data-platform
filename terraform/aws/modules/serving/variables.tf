variable "name_prefix" {
  description = "Prefix for resource names (project + env); IAM role names must keep it for the deploy role to manage them."
  type        = string

  validation {
    condition     = length(var.name_prefix) <= 28
    error_message = "name_prefix must be at most 28 characters (ALB/target group names are capped at 32)."
  }
}

variable "log_group_name" {
  description = "CloudWatch log group the serving container logs to."
  type        = string
}

variable "vpc_id" {
  description = "VPC for the ALB and the Fargate tasks."
  type        = string
}

variable "subnet_ids" {
  description = "Public subnets (at least two AZs) for the ALB and the Fargate tasks."
  type        = list(string)
}

variable "allowed_cidr" {
  description = "Only CIDR allowed to reach the ALB (e.g. your public IP /32)."
  type        = string
  sensitive   = true

  validation {
    condition     = can(cidrhost(var.allowed_cidr, 0)) && var.allowed_cidr != "0.0.0.0/0"
    error_message = "allowed_cidr must be a valid IPv4 CIDR narrower than 0.0.0.0/0."
  }
}

variable "artifacts_bucket_arn" {
  description = "ARN of the artifacts bucket; the task role reads its model prefix."
  type        = string
}

variable "model_prefix" {
  description = "Prefix in the artifacts bucket holding the model artefacts (no leading slash)."
  type        = string
  default     = "models/"
}

variable "image_tag" {
  description = "Tag of the serving image in the ECR repository (the git SHA pushed by CI)."
  type        = string
}

variable "enable_serving" {
  description = "Create the ALB, target group, listener and ECS service; false keeps only ECR, cluster and task definition (~$0 idle)."
  type        = bool
  default     = false
}

variable "desired_count" {
  description = "Number of running tasks; 0 keeps the service defined at no compute cost."
  type        = number
  default     = 0
}

variable "cpu" {
  description = "Fargate task vCPU units."
  type        = number
  default     = 1024
}

variable "memory" {
  description = "Fargate task memory (MiB)."
  type        = number
  default     = 2048
}

variable "force_delete" {
  description = "Allow destroying the ECR repository while it still holds images."
  type        = bool
  default     = true
}
