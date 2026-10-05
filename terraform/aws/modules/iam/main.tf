data "aws_caller_identity" "current" {}

data "aws_partition" "current" {}

data "aws_region" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  partition  = data.aws_partition.current.partition
  region     = data.aws_region.current.region
  # both workflows' credentialed jobs run in environment `cloud` (deployment branches: main only)
  github_subjects = ["repo:${var.github_repo}:environment:cloud"]
}

# Thumbprint-less: AWS validates token.actions.githubusercontent.com against its trusted root CAs.
resource "aws_iam_openid_connect_provider" "github" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
}

data "aws_iam_policy_document" "github_trust" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values   = local.github_subjects
    }
  }
}

resource "aws_iam_role" "github_deploy" {
  name               = "${var.name_prefix}-github-deploy"
  description        = "Read-only terraform plan from GitHub Actions (environment cloud); applies run from an admin CLI profile."
  assume_role_policy = data.aws_iam_policy_document.github_trust.json
}

# Plan-only: refresh reads, no writes. The plan job runs with -lock=false, so no lock table access;
# no object reads outside the state bucket and no secret values.
data "aws_iam_policy_document" "github_deploy" {
  statement {
    sid       = "StateBucketList"
    actions   = ["s3:ListBucket"]
    resources = ["arn:${local.partition}:s3:::${var.tf_state_bucket}"]
  }

  statement {
    sid       = "StateRead"
    actions   = ["s3:GetObject"]
    resources = ["arn:${local.partition}:s3:::${var.tf_state_bucket}/*"]
  }

  statement {
    sid       = "ProjectBucketConfig"
    actions   = ["s3:Get*"]
    resources = ["arn:${local.partition}:s3:::${var.name_prefix}-*"]
  }

  statement {
    sid     = "ProjectIamRead"
    actions = ["iam:Get*", "iam:List*"]
    resources = [
      "arn:${local.partition}:iam::${local.account_id}:role/${var.name_prefix}-*",
      aws_iam_openid_connect_provider.github.arn,
    ]
  }

  statement {
    sid       = "ProjectBudgetRead"
    actions   = ["budgets:ViewBudget", "budgets:ListTagsForResource"]
    resources = ["arn:${local.partition}:budgets::${local.account_id}:budget/${var.name_prefix}-*"]
  }

  statement {
    sid       = "ProjectTopicsRead"
    actions   = ["sns:GetTopicAttributes", "sns:GetSubscriptionAttributes", "sns:ListTagsForResource"]
    resources = ["arn:${local.partition}:sns:${local.region}:${local.account_id}:${var.name_prefix}-*"]
  }

  statement {
    sid       = "ProjectSecretMetadata"
    actions   = ["secretsmanager:DescribeSecret", "secretsmanager:GetResourcePolicy"]
    resources = ["arn:${local.partition}:secretsmanager:${local.region}:${local.account_id}:secret:${var.name_prefix}-*"]
  }

  statement {
    sid = "ProjectRepositoryRead"
    actions = [
      "ecr:DescribeRepositories",
      "ecr:GetLifecyclePolicy",
      "ecr:GetRepositoryPolicy",
      "ecr:ListTagsForResource",
    ]
    resources = ["arn:${local.partition}:ecr:${local.region}:${local.account_id}:repository/${var.name_prefix}-*"]
  }

  statement {
    sid = "RegionalDescribe"
    actions = [
      "emr-serverless:GetApplication",
      "emr-serverless:ListTagsForResource",
      "ecs:Describe*",
      "ecs:List*",
      "elasticloadbalancing:Describe*",
      "ec2:Describe*",
      "logs:Describe*",
      "logs:ListTagsForResource",
      "logs:ListTagsLogGroup",
      "cloudwatch:DescribeAlarms",
      "cloudwatch:ListTagsForResource",
    ]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "aws:RequestedRegion"
      values   = [local.region]
    }
  }
}

resource "aws_iam_role_policy" "github_deploy" {
  name   = "terraform"
  role   = aws_iam_role.github_deploy.id
  policy = data.aws_iam_policy_document.github_deploy.json
}

resource "aws_iam_role" "github_run" {
  name               = "${var.name_prefix}-github-run"
  description        = "Cloud batch from GitHub Actions: upload job code, start EMR Serverless runs, push the serving image."
  assume_role_policy = data.aws_iam_policy_document.github_trust.json
}

data "aws_iam_policy_document" "github_run" {
  statement {
    sid       = "ArtifactsPut"
    actions   = ["s3:PutObject"]
    resources = ["${var.artifacts_bucket_arn}/*"]
  }

  # cloud-batch downloads the export manifest to know which parts Snowpipe must load
  statement {
    sid       = "ExportManifestsRead"
    actions   = ["s3:GetObject"]
    resources = ["${var.lakehouse_bucket_arn}/export/_manifests/*"]
  }

  statement {
    sid       = "PassEmrJobRole"
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.emr_job.arn]

    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["emr-serverless.amazonaws.com"]
    }
  }

  dynamic "statement" {
    for_each = var.emr_application_arn == "" ? [] : [1]

    content {
      sid       = "EmrJobRuns"
      actions   = ["emr-serverless:StartJobRun", "emr-serverless:GetJobRun"]
      resources = [var.emr_application_arn, "${var.emr_application_arn}/jobruns/*"]
    }
  }

  dynamic "statement" {
    for_each = var.ecr_repository_arn == "" ? [] : [1]

    content {
      sid       = "EcrAuth"
      actions   = ["ecr:GetAuthorizationToken"]
      resources = ["*"]
    }
  }

  dynamic "statement" {
    for_each = var.ecr_repository_arn == "" ? [] : [1]

    content {
      sid = "EcrPush"
      actions = [
        "ecr:BatchCheckLayerAvailability",
        "ecr:InitiateLayerUpload",
        "ecr:UploadLayerPart",
        "ecr:CompleteLayerUpload",
        "ecr:PutImage",
      ]
      resources = [var.ecr_repository_arn]
    }
  }
}

resource "aws_iam_role_policy" "github_run" {
  name   = "cloud-batch"
  role   = aws_iam_role.github_run.id
  policy = data.aws_iam_policy_document.github_run.json
}

data "aws_iam_policy_document" "emr_trust" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

resource "aws_iam_role" "emr_job" {
  name               = "${var.name_prefix}-emr-job"
  description        = "Runtime role of the EMR Serverless silver/export job runs."
  assume_role_policy = data.aws_iam_policy_document.emr_trust.json
}

data "aws_iam_policy_document" "emr_job" {
  statement {
    sid       = "ListBuckets"
    actions   = ["s3:ListBucket"]
    resources = [var.artifacts_bucket_arn, var.lakehouse_bucket_arn]
  }

  statement {
    sid       = "ArtifactsRead"
    actions   = ["s3:GetObject"]
    resources = ["${var.artifacts_bucket_arn}/*"]
  }

  statement {
    sid       = "LakehouseReadWrite"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = ["${var.lakehouse_bucket_arn}/*"]
  }

  statement {
    sid       = "LogGroupsDescribe"
    actions   = ["logs:DescribeLogGroups"]
    resources = ["arn:${local.partition}:logs:${local.region}:${local.account_id}:log-group:*"]
  }

  statement {
    sid       = "JobLogs"
    actions   = ["logs:CreateLogStream", "logs:DescribeLogStreams", "logs:PutLogEvents"]
    resources = ["${var.emr_log_group_arn}:*"]
  }
}

resource "aws_iam_role_policy" "emr_job" {
  name   = "lakehouse"
  role   = aws_iam_role.emr_job.id
  policy = data.aws_iam_policy_document.emr_job.json
}
