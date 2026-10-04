locals {
  buckets = toset(["landing", "lakehouse", "artifacts"])
}

resource "aws_s3_bucket" "this" {
  for_each = local.buckets

  bucket        = "${var.name_prefix}-${each.key}"
  force_destroy = var.force_destroy
}

resource "aws_s3_bucket_versioning" "this" {
  for_each = local.buckets

  bucket = aws_s3_bucket.this[each.key].id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = local.buckets

  bucket = aws_s3_bucket.this[each.key].id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each = local.buckets

  bucket = aws_s3_bucket.this[each.key].id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "this" {
  for_each = local.buckets

  bucket = aws_s3_bucket.this[each.key].id

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_version_expiration_days
    }
  }

  dynamic "rule" {
    for_each = each.key == "landing" ? [1] : []

    content {
      id     = "expire-landing"
      status = "Enabled"

      filter {}

      expiration {
        days = var.landing_expiration_days
      }
    }
  }

  depends_on = [aws_s3_bucket_versioning.this]
}
