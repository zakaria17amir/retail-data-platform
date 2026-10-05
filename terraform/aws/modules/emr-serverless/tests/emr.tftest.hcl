mock_provider "aws" {}

variables {
  name_prefix    = "retail-data-platform-demo"
  release_label  = "emr-spark-8.0.0"
  log_group_name = "/retail-data-platform-demo/emr-serverless"
}

run "guard_rails" {
  command = plan
  assert {
    condition     = one(aws_emrserverless_application.spark.auto_stop_configuration).idle_timeout_minutes == 5
    error_message = "auto-stop after 5 idle minutes"
  }
  assert {
    condition     = one(aws_emrserverless_application.spark.maximum_capacity).cpu == "16 vCPU" && one(aws_emrserverless_application.spark.maximum_capacity).memory == "64 GB"
    error_message = "max capacity 16 vCPU / 64 GB"
  }
  assert {
    condition     = one(one(aws_emrserverless_application.spark.monitoring_configuration).cloudwatch_logging_configuration).log_group_name == "/retail-data-platform-demo/emr-serverless"
    error_message = "cloudwatch logging to the given group"
  }
}
