locals {
  log_groups = toset(["emr-serverless", "serving"])
}

resource "aws_budgets_budget" "monthly" {
  name         = "${var.name_prefix}-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  dynamic "notification" {
    for_each = toset(var.budget_alert_thresholds)

    content {
      comparison_operator        = "GREATER_THAN"
      notification_type          = "ACTUAL"
      threshold                  = notification.value
      threshold_type             = "PERCENTAGE"
      subscriber_email_addresses = [var.alert_email]
    }
  }
}

resource "aws_sns_topic" "alerts" {
  name = "${var.name_prefix}-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_cloudwatch_log_group" "this" {
  for_each = local.log_groups

  name              = "/${var.name_prefix}/${each.key}"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_metric_filter" "job_errors" {
  name           = "${var.name_prefix}-job-errors"
  log_group_name = aws_cloudwatch_log_group.this["emr-serverless"].name
  pattern        = "Traceback"

  metric_transformation {
    name      = "JobErrors"
    namespace = var.name_prefix
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "job_errors" {
  alarm_name          = "${var.name_prefix}-job-errors"
  alarm_description   = "A Python traceback was logged by an EMR Serverless job run."
  namespace           = var.name_prefix
  metric_name         = aws_cloudwatch_log_metric_filter.job_errors.metric_transformation[0].name
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}
