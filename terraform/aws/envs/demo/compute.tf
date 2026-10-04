variable "emr_release_label" {
  description = "EMR Serverless release for the silver jobs: emr-spark-8.0.0 = Spark 4.0.2 / Delta 4.0.0 (local: Spark 4.0.4 / Delta 4.0.1); confirmed by Phase 7 T4."
  type        = string
  default     = "emr-spark-8.0.0"
}

variable "serving_image_tag" {
  description = "Serving image tag in ECR (the git SHA CI pushes)."
  type        = string
}

variable "serving_allowed_cidr" {
  description = "Only CIDR allowed to reach the serving ALB (e.g. your public IP /32)."
  type        = string
}

variable "serving_desired_count" {
  description = "Running serving tasks; 0 = defined but no compute cost."
  type        = number
  default     = 0
}

variable "snowflake_sqs_arn" {
  description = "Snowflake pipe notification_channel (SQS ARN), from the Snowflake root's outputs; empty skips the S3 notification."
  type        = string
  default     = ""
}

variable "snowflake_iam_user_arn" {
  description = "Snowflake storage integration STORAGE_AWS_IAM_USER_ARN; empty skips the Snowflake role (step 1 of the two-step apply)."
  type        = string
  default     = ""
}

variable "snowflake_external_id" {
  description = "Snowflake storage integration STORAGE_AWS_EXTERNAL_ID; empty skips the Snowflake role (step 1 of the two-step apply)."
  type        = string
  default     = ""
}

# demo: the default VPC's public subnets; tasks get a public IP, so no NAT gateway to pay for
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }

  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

module "emr_serverless" {
  source = "../../modules/emr-serverless"

  name_prefix    = local.name_prefix
  release_label  = var.emr_release_label
  log_group_name = module.observability.emr_log_group_name
}

module "serving" {
  source = "../../modules/serving"

  name_prefix          = local.name_prefix
  vpc_id               = data.aws_vpc.default.id
  subnet_ids           = data.aws_subnets.default.ids
  allowed_cidr         = var.serving_allowed_cidr
  artifacts_bucket_arn = module.storage.artifacts_bucket_arn
  log_group_name       = module.observability.serving_log_group_name
  image_tag            = var.serving_image_tag
  desired_count        = var.serving_desired_count
}

module "snowpipe_integration" {
  source = "../../modules/snowpipe-integration"

  name_prefix            = local.name_prefix
  bucket                 = module.storage.lakehouse_bucket
  bucket_arn             = module.storage.lakehouse_bucket_arn
  snowflake_sqs_arn      = var.snowflake_sqs_arn
  snowflake_iam_user_arn = var.snowflake_iam_user_arn
  snowflake_external_id  = var.snowflake_external_id
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
  description = "ECS serving service (scale with aws ecs update-service --desired-count)."
  value       = module.serving.service_name
}

output "serving_url" {
  description = "Serving base URL (ALB, allowed CIDR only)."
  value       = "http://${module.serving.alb_dns_name}"
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
