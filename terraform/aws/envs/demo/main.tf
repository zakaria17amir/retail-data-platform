data "aws_caller_identity" "current" {}

locals {
  name_prefix = "retail-data-platform-${var.env}"
}

module "storage" {
  source = "../../modules/storage"

  name_prefix   = "${local.name_prefix}-${data.aws_caller_identity.current.account_id}"
  force_destroy = var.force_destroy
}

module "observability" {
  source = "../../modules/observability"

  name_prefix        = local.name_prefix
  alert_email        = var.alert_email
  monthly_budget_usd = var.monthly_budget_usd
}

module "iam" {
  source = "../../modules/iam"

  name_prefix          = local.name_prefix
  github_repo          = var.github_repo
  tf_state_bucket      = var.tf_state_bucket
  tf_lock_table        = var.tf_lock_table
  artifacts_bucket_arn = module.storage.artifacts_bucket_arn
  lakehouse_bucket_arn = module.storage.lakehouse_bucket_arn
  emr_log_group_arn    = module.observability.emr_log_group_arn
}
