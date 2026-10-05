variable "name_prefix" {
  description = "Prefix for the role name (project + env); keeps it manageable by the deploy role."
  type        = string
}

variable "bucket" {
  description = "Name of the lakehouse bucket holding the Parquet export."
  type        = string
}

variable "bucket_arn" {
  description = "ARN of the lakehouse bucket holding the Parquet export."
  type        = string
}

variable "export_prefix" {
  description = "Key prefix of the Parquet export Snowpipe ingests (trailing slash)."
  type        = string
  default     = "export/silver/"
}

variable "snowflake_sqs_arn" {
  description = "Snowflake-managed SQS queue ARN (a pipe's notification_channel); empty skips the S3 notification."
  type        = string
  default     = ""
}

variable "snowflake_iam_user_arn" {
  description = "STORAGE_AWS_IAM_USER_ARN of the Snowflake storage integration; empty skips the role."
  type        = string
  default     = ""
}

variable "snowflake_external_id" {
  description = "STORAGE_AWS_EXTERNAL_ID of the Snowflake storage integration; empty skips the role."
  type        = string
  default     = ""
}
