variable "region" {
  description = "AWS region for the state bucket and lock table."
  type        = string
  default     = "eu-west-1"
}

variable "env" {
  description = "Environment name used for the env tag."
  type        = string
  default     = "demo"
}

variable "state_bucket_name" {
  description = "Globally unique name of the Terraform remote state bucket."
  type        = string
}

variable "lock_table_name" {
  description = "Name of the DynamoDB table used for Terraform state locking."
  type        = string
  default     = "retail-data-platform-tf-locks"
}
