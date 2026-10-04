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
}

variable "tf_state_bucket" {
  description = "Name of the Terraform state bucket created by terraform/aws/bootstrap."
  type        = string
}

variable "tf_lock_table" {
  description = "Name of the DynamoDB lock table created by terraform/aws/bootstrap."
  type        = string
  default     = "retail-data-platform-tf-locks"
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
