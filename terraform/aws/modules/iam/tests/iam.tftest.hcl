mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = { account_id = "123456789012" }
  }
  mock_data "aws_partition" {
    defaults = { partition = "aws" }
  }
  mock_data "aws_region" {
    defaults = { region = "eu-west-1" }
  }
  mock_data "aws_iam_policy_document" {
    defaults = { json = "{}" }
  }
  mock_resource "aws_iam_openid_connect_provider" {
    defaults = { arn = "arn:aws:iam::123456789012:oidc-provider/token.actions.githubusercontent.com" }
  }
  mock_resource "aws_iam_role" {
    defaults = { arn = "arn:aws:iam::123456789012:role/mock" }
  }
}

variables {
  name_prefix          = "retail-data-platform-demo"
  github_repo          = "owner/retail-data-platform"
  tf_state_bucket      = "tfstate"
  artifacts_bucket_arn = "arn:aws:s3:::art"
  lakehouse_bucket_arn = "arn:aws:s3:::lake"
  emr_log_group_arn    = "arn:aws:logs:eu-west-1:123456789012:log-group:/emr"
}

run "plan_role_is_read_only" {
  command = apply # mocked provider: no API calls
  assert {
    condition     = !contains(flatten([for s in data.aws_iam_policy_document.github_deploy.statement : s.actions]), "iam:*")
    error_message = "the plan role must not hold iam:*"
  }
  assert {
    condition     = alltrue([for a in flatten([for s in data.aws_iam_policy_document.github_deploy.statement : s.actions]) : can(regex("^[a-z0-9-]+:(Get|List|Describe|View)[A-Za-z]*\\*?$", a))])
    error_message = "the plan role may only hold Get/List/Describe/View actions"
  }
  assert {
    condition     = alltrue([for s in data.aws_iam_policy_document.github_deploy.statement : !contains(s.actions, "s3:GetObject") || toset(s.resources) == toset(["arn:aws:s3:::tfstate/*"])])
    error_message = "object reads only in the state bucket"
  }
}

run "oidc_trusts_only_the_cloud_environment" {
  command = apply
  assert {
    condition     = toset(one([for c in data.aws_iam_policy_document.github_trust.statement[0].condition : c.values if c.variable == "token.actions.githubusercontent.com:sub"])) == toset(["repo:owner/retail-data-platform:environment:cloud"])
    error_message = "trust must be exactly repo:<repo>:environment:cloud"
  }
}
