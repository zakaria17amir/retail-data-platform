variable "name_prefix" {
  description = "Globally unique prefix for bucket names; buckets are <name_prefix>-landing|lakehouse|artifacts."
  type        = string
}

variable "force_destroy" {
  description = "Allow destroying non-empty buckets (true for the tear-down-able demo)."
  type        = bool
  default     = false
}

variable "landing_expiration_days" {
  description = "Days after which objects in the landing bucket expire."
  type        = number
  default     = 7
}

variable "noncurrent_version_expiration_days" {
  description = "Days after which noncurrent object versions expire in every bucket."
  type        = number
  default     = 7
}
