variable "region" {
  description = "AWS region of the demo environment."
  type        = string
  default     = "eu-west-1"
}

variable "env" {
  description = "Environment name, used in resource names and the env tag."
  type        = string
  default     = "demo"
}

variable "github_repo" {
  description = "GitHub repository (<owner>/<repo>) trusted by the OIDC roles."
  type        = string
}

variable "alert_email" {
  description = "Email address for budget and alarm notifications."
  type        = string
  sensitive   = true
}

variable "tf_state_bucket" {
  description = "Name of the Terraform state bucket created by terraform/aws/bootstrap (the plan role reads it)."
  type        = string
}

variable "force_destroy" {
  description = "Allow destroying non-empty demo buckets so make cloud-down tears everything down."
  type        = bool
  default     = true
}

variable "monthly_budget_usd" {
  description = "Monthly AWS cost budget in USD."
  type        = number
  default     = 25
}

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
  sensitive   = true
}

variable "enable_serving" {
  description = "Create the serving ALB + ECS service (ALB ~$17/month idle); false keeps ECR + cluster only."
  type        = bool
  default     = false
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
