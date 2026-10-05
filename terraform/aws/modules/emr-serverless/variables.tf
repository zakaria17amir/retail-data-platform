variable "name_prefix" {
  description = "Prefix for resource names (project + env)."
  type        = string
}

variable "release_label" {
  description = "EMR Serverless release label; must ship the Spark/Delta versions the silver jobs are built against."
  type        = string
}

variable "log_group_name" {
  description = "CloudWatch log group for Spark driver/executor logs (the job role must be allowed to write to it)."
  type        = string
}

variable "max_cpu" {
  description = "Maximum total vCPU the application may scale to."
  type        = string
  default     = "16 vCPU"
}

variable "max_memory" {
  description = "Maximum total memory the application may scale to."
  type        = string
  default     = "64 GB"
}

variable "idle_timeout_minutes" {
  description = "Minutes of inactivity after which the application auto-stops."
  type        = number
  default     = 5
}
