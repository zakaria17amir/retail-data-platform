variable "organization_name" {
  description = "Snowflake organization name (the <org> in <org>-<account>)."
  type        = string
  sensitive   = true
}

variable "account_name" {
  description = "Snowflake account name (the <account> in <org>-<account>)."
  type        = string
  sensitive   = true
}

variable "user" {
  description = "Snowflake user Terraform authenticates as (key-pair auth)."
  type        = string
}

variable "private_key" {
  description = "PEM-encoded private key of var.user (unencrypted, literal newlines)."
  type        = string
  sensitive   = true
}

variable "role" {
  description = "Role Terraform runs as; storage integrations and resource monitors need ACCOUNTADMIN."
  type        = string
  default     = "ACCOUNTADMIN"
}

variable "lakehouse_bucket" {
  description = "Name of the S3 lakehouse bucket (module.storage output lakehouse_bucket); Snowflake reads its export/silver/ prefix only."
  type        = string
}

variable "storage_aws_role_arn" {
  description = "ARN of the AWS IAM role the storage integration assumes (module.snowpipe_integration output snowflake_role_arn). Two-step apply: pass the planned ARN first, then create the AWS role from this root's outputs."
  type        = string
}

variable "dbt_rsa_public_key" {
  description = "RSA public key of the dbt service user, one line without the BEGIN/END header and trailer."
  type        = string
}

variable "credit_quota" {
  description = "Monthly credit quota of the warehouse resource monitor; the warehouse is suspended at 100 %."
  type        = number
  default     = 5
}
