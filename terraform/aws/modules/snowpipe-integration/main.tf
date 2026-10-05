locals {
  role_name   = "${var.name_prefix}-snowflake-export-read"
  create_role = var.snowflake_iam_user_arn != "" && var.snowflake_external_id != ""
}

data "aws_caller_identity" "current" {}

data "aws_partition" "current" {}

# Direct S3 -> Snowflake-managed SQS (Snowflake's documented auto-ingest path). All pipes of the
# account share that queue, so one notification serves every silver table.
resource "aws_s3_bucket_notification" "export" {
  count  = var.snowflake_sqs_arn != "" ? 1 : 0
  bucket = var.bucket

  queue {
    queue_arn     = var.snowflake_sqs_arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = var.export_prefix
    filter_suffix = ".parquet"
  }
}

resource "aws_iam_role" "snowflake" {
  count = local.create_role ? 1 : 0
  name  = local.role_name
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { AWS = var.snowflake_iam_user_arn }
      Condition = { StringEquals = { "sts:ExternalId" = var.snowflake_external_id } }
    }]
  })
}

resource "aws_iam_role_policy" "snowflake" {
  count = local.create_role ? 1 : 0
  name  = "read-export"
  role  = aws_iam_role.snowflake[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:GetObjectVersion"]
        Resource = ["${var.bucket_arn}/${var.export_prefix}*"]
      },
      {
        Effect    = "Allow"
        Action    = ["s3:ListBucket"]
        Resource  = [var.bucket_arn]
        Condition = { StringLike = { "s3:prefix" = ["${var.export_prefix}*"] } }
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation"]
        Resource = [var.bucket_arn]
      },
    ]
  })
}
