# demo: the default VPC's public subnets; tasks get a public IP, so no NAT gateway to pay for
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }

  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

module "emr_serverless" {
  source = "../../modules/emr-serverless"

  name_prefix    = local.name_prefix
  release_label  = var.emr_release_label
  log_group_name = module.observability.emr_log_group_name
}

module "serving" {
  source = "../../modules/serving"

  name_prefix          = local.name_prefix
  vpc_id               = data.aws_vpc.default.id
  subnet_ids           = data.aws_subnets.default.ids
  allowed_cidr         = var.serving_allowed_cidr
  artifacts_bucket_arn = module.storage.artifacts_bucket_arn
  log_group_name       = module.observability.serving_log_group_name
  image_tag            = var.serving_image_tag
  enable_serving       = var.enable_serving
  desired_count        = var.serving_desired_count
}

module "snowpipe_integration" {
  source = "../../modules/snowpipe-integration"

  name_prefix            = local.name_prefix
  bucket                 = module.storage.lakehouse_bucket
  bucket_arn             = module.storage.lakehouse_bucket_arn
  snowflake_sqs_arn      = var.snowflake_sqs_arn
  snowflake_iam_user_arn = var.snowflake_iam_user_arn
  snowflake_external_id  = var.snowflake_external_id
}
