locals {
  comment = "retail-data-platform (demo)"
  roles   = toset(["LOADER", "TRANSFORMER", "REPORTER"])
}

resource "snowflake_database" "retail" {
  name    = "RETAIL"
  comment = local.comment
}

resource "snowflake_schema" "this" {
  for_each = toset(["RAW", "SILVER", "GOLD"])
  database = snowflake_database.retail.name
  name     = each.key
  comment  = local.comment
}

resource "snowflake_resource_monitor" "retail" {
  name                      = "RETAIL_MONITOR"
  credit_quota              = var.credit_quota
  notify_triggers           = [50, 80]
  suspend_immediate_trigger = 100
}

resource "snowflake_warehouse" "retail" {
  name                = "RETAIL_WH"
  warehouse_size      = "XSMALL"
  auto_suspend        = 60
  auto_resume         = "true"
  initially_suspended = true
  resource_monitor    = snowflake_resource_monitor.retail.fully_qualified_name
  comment             = local.comment
}

resource "snowflake_account_role" "this" {
  for_each = local.roles
  name     = each.key
  comment  = local.comment
}

# keeps the Terraform role (ACCOUNTADMIN > SYSADMIN) in control of objects these roles own
resource "snowflake_grant_account_role" "to_sysadmin" {
  for_each         = local.roles
  role_name        = snowflake_account_role.this[each.key].name
  parent_role_name = "SYSADMIN"
}

resource "snowflake_grant_privileges_to_account_role" "database_usage" {
  for_each          = local.roles
  account_role_name = snowflake_account_role.this[each.key].name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.retail.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "warehouse_usage" {
  for_each          = toset(["TRANSFORMER", "REPORTER"])
  account_role_name = snowflake_account_role.this[each.key].name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.retail.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "schema_usage" {
  for_each = {
    LOADER      = "SILVER"
    TRANSFORMER = "SILVER"
    REPORTER    = "GOLD"
  }
  account_role_name = snowflake_account_role.this[each.key].name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = snowflake_schema.this[each.value].fully_qualified_name
  }
}

resource "snowflake_grant_ownership" "gold" {
  account_role_name   = snowflake_account_role.this["TRANSFORMER"].name
  outbound_privileges = "COPY"
  on {
    object_type = "SCHEMA"
    object_name = snowflake_schema.this["GOLD"].fully_qualified_name
  }
  depends_on = [snowflake_grant_account_role.to_sysadmin]
}

resource "snowflake_grant_privileges_to_account_role" "reporter_gold" {
  for_each          = toset(["TABLES", "VIEWS"])
  account_role_name = snowflake_account_role.this["REPORTER"].name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = each.key
      in_schema          = snowflake_schema.this["GOLD"].fully_qualified_name
    }
  }
}

resource "snowflake_storage_integration_aws" "export" {
  name                      = "RETAIL_S3_EXPORT"
  enabled                   = true
  storage_provider          = "S3"
  storage_aws_role_arn      = var.storage_aws_role_arn
  storage_allowed_locations = ["s3://${var.lakehouse_bucket}/export/silver/"]
  comment                   = local.comment
}

resource "snowflake_stage_external_s3" "export" {
  database            = snowflake_database.retail.name
  schema              = snowflake_schema.this["SILVER"].name
  name                = "EXPORT_STAGE"
  url                 = "s3://${var.lakehouse_bucket}/export/silver/"
  storage_integration = snowflake_storage_integration_aws.export.name
  comment             = local.comment

  file_format {
    parquet {
      use_logical_type = "true"
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "loader_stage" {
  account_role_name = snowflake_account_role.this["LOADER"].name
  privileges        = ["USAGE"]
  on_schema_object {
    object_type = "STAGE"
    object_name = snowflake_stage_external_s3.export.fully_qualified_name
  }
}

resource "snowflake_service_user" "dbt" {
  name              = "DBT_SERVICE"
  comment           = local.comment
  default_role      = snowflake_account_role.this["TRANSFORMER"].name
  default_warehouse = snowflake_warehouse.retail.name
  default_namespace = "${snowflake_database.retail.name}.${snowflake_schema.this["GOLD"].name}"
  rsa_public_key    = var.dbt_rsa_public_key
}

resource "snowflake_grant_account_role" "dbt_transformer" {
  role_name = snowflake_account_role.this["TRANSFORMER"].name
  user_name = snowflake_service_user.dbt.name
}
