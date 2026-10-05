terraform {
  required_version = ">= 1.9"

  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "2.21.0"
    }
  }

  # partial config: terraform init -backend-config=backend.hcl
  backend "s3" {}
}

provider "snowflake" {
  organization_name = var.organization_name
  account_name      = var.account_name
  user              = var.user
  role              = var.role
  authenticator     = "SNOWFLAKE_JWT"
  private_key       = var.private_key

  preview_features_enabled = ["snowflake_table_resource", "snowflake_pipe_resource"]
}
