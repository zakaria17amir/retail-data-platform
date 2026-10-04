resource "aws_emrserverless_application" "spark" {
  name          = "${var.name_prefix}-spark"
  release_label = var.release_label
  type          = "spark"

  auto_start_configuration {
    enabled = true
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = var.idle_timeout_minutes
  }

  maximum_capacity {
    cpu    = var.max_cpu
    memory = var.max_memory
  }

  monitoring_configuration {
    cloudwatch_logging_configuration {
      enabled        = true
      log_group_name = var.log_group_name
    }
  }
}
