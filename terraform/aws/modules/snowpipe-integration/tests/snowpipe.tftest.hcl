mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = { account_id = "123456789012" }
  }
  mock_data "aws_partition" {
    defaults = { partition = "aws" }
  }
}

variables {
  name_prefix = "retail-data-platform-demo"
  bucket      = "lake"
  bucket_arn  = "arn:aws:s3:::lake"
}

run "empty_inputs_skip_role_and_notification" {
  command = plan
  assert {
    condition     = length(aws_iam_role.snowflake) == 0 && length(aws_s3_bucket_notification.export) == 0
    error_message = "role/notification must be skipped when snowflake inputs are empty"
  }
  assert {
    condition     = output.snowflake_role_arn == "arn:aws:iam::123456789012:role/retail-data-platform-demo-snowflake-export-read"
    error_message = "predicted role ARN wrong"
  }
  assert {
    condition     = output.sqs_queue_arn == null
    error_message = "sqs_queue_arn must be null when unset"
  }
}

run "only_user_arn_still_skips_role" {
  command = plan
  variables {
    snowflake_iam_user_arn = "arn:aws:iam::999999999999:user/sf"
  }
  assert {
    condition     = length(aws_iam_role.snowflake) == 0
    error_message = "role needs both user arn and external id"
  }
}

run "full_inputs_create_scoped_role_and_notification" {
  command = plan
  variables {
    snowflake_sqs_arn      = "arn:aws:sqs:eu-west-1:999999999999:sf-snowpipe"
    snowflake_iam_user_arn = "arn:aws:iam::999999999999:user/sf"
    snowflake_external_id  = "EXT_ID"
  }
  assert {
    condition     = jsondecode(aws_iam_role.snowflake[0].assume_role_policy).Statement[0].Condition.StringEquals["sts:ExternalId"] == "EXT_ID"
    error_message = "trust must require the external id"
  }
  assert {
    condition     = jsondecode(aws_iam_role.snowflake[0].assume_role_policy).Statement[0].Principal.AWS == "arn:aws:iam::999999999999:user/sf"
    error_message = "trust must name the snowflake user"
  }
  assert {
    condition     = jsondecode(aws_iam_role_policy.snowflake[0].policy).Statement[0].Resource[0] == "arn:aws:s3:::lake/export/silver/*"
    error_message = "read must be scoped to the export prefix"
  }
  assert {
    condition     = one(aws_s3_bucket_notification.export[0].queue).filter_prefix == "export/silver/" && one(aws_s3_bucket_notification.export[0].queue).filter_suffix == ".parquet"
    error_message = "notification filter wrong"
  }
  assert {
    condition     = one(aws_s3_bucket_notification.export[0].queue).queue_arn == "arn:aws:sqs:eu-west-1:999999999999:sf-snowpipe"
    error_message = "notification must target the snowflake queue"
  }
}
