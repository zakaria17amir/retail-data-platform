variable "name_prefix" {
  description = "Prefix for role names; the deploy role may only manage project resources carrying this prefix."
  type        = string
}

variable "github_repo" {
  description = "GitHub repository allowed to assume the roles, as <owner>/<repo> (no wildcards)."
  type        = string

  validation {
    condition     = can(regex("^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", var.github_repo))
    error_message = "github_repo must be <owner>/<repo> without wildcards."
  }
}

variable "tf_state_bucket" {
  description = "Name of the Terraform remote state bucket (from bootstrap)."
  type        = string
}

variable "tf_lock_table" {
  description = "Name of the DynamoDB state lock table (from bootstrap)."
  type        = string
}

variable "artifacts_bucket_arn" {
  description = "ARN of the artifacts bucket (job code, model artefacts)."
  type        = string
}

variable "lakehouse_bucket_arn" {
  description = "ARN of the lakehouse bucket."
  type        = string
}

variable "emr_log_group_arn" {
  description = "ARN of the CloudWatch log group EMR Serverless job runs write to."
  type        = string
}

variable "emr_application_arn" {
  description = "ARN of the EMR Serverless application the run role may start jobs on; empty grants nothing."
  type        = string
  default     = ""
}

variable "ecr_repository_arn" {
  description = "ARN of the ECR repository the run role may push to; empty grants nothing."
  type        = string
  default     = ""
}
