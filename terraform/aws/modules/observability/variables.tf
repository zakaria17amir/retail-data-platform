variable "name_prefix" {
  description = "Prefix for budget, topic, log group and alarm names."
  type        = string
}

variable "alert_email" {
  description = "Email address receiving budget and alarm notifications."
  type        = string
  sensitive   = true
}

variable "monthly_budget_usd" {
  description = "Monthly AWS cost budget in USD."
  type        = number
  default     = 25
}

variable "budget_alert_thresholds" {
  description = "Actual-spend percentages of the budget that trigger an email."
  type        = list(number)
  default     = [50, 80, 100]
}

variable "log_retention_days" {
  description = "Retention of the CloudWatch log groups in days."
  type        = number
  default     = 7
}
