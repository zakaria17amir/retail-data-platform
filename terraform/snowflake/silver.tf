# Silver contract (lakehouse_spark/silver/tables.py + job.py; docs/superpowers/plans/phase-2-lakehouse.md).
# Spark → Snowflake: string VARCHAR, int/bigint NUMBER(38,0), double FLOAT, decimal(12,2) NUMBER(12,2),
# boolean BOOLEAN, date DATE, timestamp TIMESTAMP_LTZ(6), timestamp_ntz TIMESTAMP_NTZ(6).
locals {
  cdc_meta = [
    "_SOURCE_LSN NUMBER(38,0)",
    "_SOURCE_TS TIMESTAMP_LTZ(6)",
    "_IS_DELETED BOOLEAN",
    "_SILVER_LOADED_AT TIMESTAMP_LTZ(6)",
    "_RUN_ID VARCHAR",
  ]
  scd2 = ["VALID_FROM TIMESTAMP_LTZ(6)", "VALID_TO TIMESTAMP_LTZ(6)", "IS_CURRENT BOOLEAN"]

  # key = Snowflake table (dbt source name), path = silver table under export/silver/
  silver = {
    categories = {
      path = "catalog/categories"
      columns = concat([
        "PRODUCT_CATEGORY_NAME VARCHAR",
        "PRODUCT_CATEGORY_NAME_ENGLISH VARCHAR",
      ], local.cdc_meta)
    }
    products = {
      path = "catalog/products"
      columns = concat([
        "PRODUCT_ID VARCHAR",
        "PRODUCT_CATEGORY_NAME VARCHAR",
        "PRODUCT_NAME_LENGTH NUMBER(38,0)",
        "PRODUCT_DESCRIPTION_LENGTH NUMBER(38,0)",
        "PRODUCT_PHOTOS_QTY NUMBER(38,0)",
        "PRODUCT_WEIGHT_G NUMBER(38,0)",
        "PRODUCT_LENGTH_CM NUMBER(38,0)",
        "PRODUCT_HEIGHT_CM NUMBER(38,0)",
        "PRODUCT_WIDTH_CM NUMBER(38,0)",
        "PRODUCT_CATEGORY_NAME_ENGLISH VARCHAR",
      ], local.cdc_meta, local.scd2)
    }
    customers = {
      path = "party/customers"
      columns = concat([
        "CUSTOMER_ID VARCHAR",
        "CUSTOMER_UNIQUE_ID VARCHAR",
        "CUSTOMER_ZIP_CODE_PREFIX NUMBER(38,0)",
        "CUSTOMER_CITY VARCHAR",
        "CUSTOMER_STATE VARCHAR",
      ], local.cdc_meta, local.scd2)
    }
    sellers = {
      path = "party/sellers"
      columns = concat([
        "SELLER_ID VARCHAR",
        "SELLER_ZIP_CODE_PREFIX NUMBER(38,0)",
        "SELLER_CITY VARCHAR",
        "SELLER_STATE VARCHAR",
      ], local.cdc_meta, local.scd2)
    }
    geolocation_points = {
      path = "geo/geolocation_points"
      columns = concat([
        "GEOLOCATION_PK NUMBER(38,0)",
        "ZIP_CODE_PREFIX NUMBER(38,0)",
        "LAT FLOAT",
        "LNG FLOAT",
        "CITY VARCHAR",
        "STATE VARCHAR",
      ], local.cdc_meta)
    }
    zip_centroids = {
      path = "geo/zip_centroids"
      columns = [
        "ZIP_CODE_PREFIX NUMBER(38,0)",
        "LAT FLOAT",
        "LNG FLOAT",
        "N_POINTS NUMBER(38,0)",
        "STATE VARCHAR",
      ]
    }
    orders = {
      path = "sales/orders"
      columns = concat([
        "ORDER_ID VARCHAR",
        "CUSTOMER_ID VARCHAR",
        "ORDER_STATUS VARCHAR",
        "ORDER_PURCHASE_TS_LOCAL TIMESTAMP_NTZ(6)",
        "ORDER_PURCHASE_TS_UTC TIMESTAMP_LTZ(6)",
        "ORDER_APPROVED_TS_LOCAL TIMESTAMP_NTZ(6)",
        "ORDER_APPROVED_TS_UTC TIMESTAMP_LTZ(6)",
        "ORDER_DELIVERED_CARRIER_TS_LOCAL TIMESTAMP_NTZ(6)",
        "ORDER_DELIVERED_CARRIER_TS_UTC TIMESTAMP_LTZ(6)",
        "ORDER_DELIVERED_CUSTOMER_TS_LOCAL TIMESTAMP_NTZ(6)",
        "ORDER_DELIVERED_CUSTOMER_TS_UTC TIMESTAMP_LTZ(6)",
        "ORDER_ESTIMATED_DELIVERY_TS_LOCAL TIMESTAMP_NTZ(6)",
        "ORDER_ESTIMATED_DELIVERY_TS_UTC TIMESTAMP_LTZ(6)",
        "FLAG_APPROVED_BEFORE_PURCHASE BOOLEAN",
        "FLAG_CARRIER_BEFORE_APPROVED BOOLEAN",
        "FLAG_DELIVERED_BEFORE_CARRIER BOOLEAN",
        "FLAG_DELIVERED_BEFORE_PURCHASE BOOLEAN",
      ], local.cdc_meta)
    }
    order_items = {
      path = "sales/order_items"
      columns = concat([
        "ORDER_ID VARCHAR",
        "ORDER_ITEM_ID NUMBER(38,0)",
        "PRODUCT_ID VARCHAR",
        "SELLER_ID VARCHAR",
        "SHIPPING_LIMIT_TS_LOCAL TIMESTAMP_NTZ(6)",
        "SHIPPING_LIMIT_TS_UTC TIMESTAMP_LTZ(6)",
        "PRICE NUMBER(12,2)",
        "FREIGHT_VALUE NUMBER(12,2)",
      ], local.cdc_meta)
    }
    order_payments = {
      path = "sales/order_payments"
      columns = concat([
        "ORDER_ID VARCHAR",
        "PAYMENT_SEQUENTIAL NUMBER(38,0)",
        "PAYMENT_TYPE VARCHAR",
        "PAYMENT_INSTALLMENTS NUMBER(38,0)",
        "PAYMENT_VALUE NUMBER(12,2)",
      ], local.cdc_meta)
    }
    order_reviews = {
      path = "sales/order_reviews"
      columns = concat([
        "REVIEW_PK NUMBER(38,0)",
        "REVIEW_ID VARCHAR",
        "ORDER_ID VARCHAR",
        "REVIEW_SCORE NUMBER(38,0)",
        "REVIEW_COMMENT_TITLE VARCHAR",
        "REVIEW_COMMENT_MESSAGE VARCHAR",
        "REVIEW_CREATION_TS_LOCAL TIMESTAMP_NTZ(6)",
        "REVIEW_CREATION_TS_UTC TIMESTAMP_LTZ(6)",
        "REVIEW_ANSWER_TS_LOCAL TIMESTAMP_NTZ(6)",
        "REVIEW_ANSWER_TS_UTC TIMESTAMP_LTZ(6)",
      ], local.cdc_meta)
    }
    clickstream = {
      path = "events/clickstream"
      columns = [
        "EVENT_ID VARCHAR",
        "EVENT_TYPE VARCHAR",
        "SESSION_ID VARCHAR",
        "CUSTOMER_ID VARCHAR",
        "DEVICE VARCHAR",
        "REFERRER VARCHAR",
        "EVENT_TS_LOCAL TIMESTAMP_NTZ(6)",
        "EVENT_TS_UTC TIMESTAMP_LTZ(6)",
        "PRODUCT_ID VARCHAR",
        "SEARCH_QUERY VARCHAR",
        "QUANTITY NUMBER(38,0)",
        "ORDER_ID VARCHAR",
        "UTM_CAMPAIGN VARCHAR",
        "EVENT_DATE DATE",
        "_BRONZE_INGEST_TS TIMESTAMP_LTZ(6)",
        "_SILVER_LOADED_AT TIMESTAMP_LTZ(6)",
        "_RUN_ID VARCHAR",
        "RANK NUMBER(38,0)",
        "REC_MODEL_VERSION VARCHAR",
        "REC_STRATEGY VARCHAR",
      ]
    }
  }
}

resource "snowflake_table" "silver" {
  for_each = local.silver
  database = snowflake_database.retail.name
  schema   = snowflake_schema.this["SILVER"].name
  name     = upper(each.key)
  comment  = "silver/${each.value.path}; Snowpipe from export/silver/${each.value.path}/"

  dynamic "column" {
    # _EXPORT_FILE (stage-relative path incl. <run_id>) tells export snapshots apart
    for_each = concat(each.value.columns, ["_EXPORT_FILE VARCHAR"])
    content {
      name = split(" ", column.value)[0]
      type = split(" ", column.value)[1]
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "loader_tables" {
  for_each          = snowflake_table.silver
  account_role_name = snowflake_account_role.this["LOADER"].name
  privileges        = ["INSERT", "SELECT"]
  on_schema_object {
    object_type = "TABLE"
    object_name = each.value.fully_qualified_name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_tables" {
  for_each          = snowflake_table.silver
  account_role_name = snowflake_account_role.this["TRANSFORMER"].name
  privileges        = ["SELECT"]
  on_schema_object {
    object_type = "TABLE"
    object_name = each.value.fully_qualified_name
  }
}

# Snowpipe keeps per-file load metadata (14 days): a re-sent notification for an already-loaded
# path is skipped, so each export file loads once.
resource "snowflake_pipe" "silver" {
  for_each    = local.silver
  database    = snowflake_database.retail.name
  schema      = snowflake_schema.this["SILVER"].name
  name        = "${upper(each.key)}_PIPE"
  auto_ingest = true
  comment     = local.comment
  copy_statement = join(" ", [
    "COPY INTO ${snowflake_table.silver[each.key].fully_qualified_name}",
    "FROM @${snowflake_stage_external_s3.export.fully_qualified_name}/${each.value.path}/",
    "PATTERN = '.*[.]parquet'",
    "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE",
    "INCLUDE_METADATA = (_EXPORT_FILE = METADATA$FILENAME)",
  ])

  lifecycle {
    replace_triggered_by = [
      snowflake_stage_external_s3.export.url,
      snowflake_stage_external_s3.export.storage_integration,
    ]
  }
}

# OPERATE + MONITOR let the provider pause, transfer and force-resume the pipe in one apply
resource "snowflake_grant_privileges_to_account_role" "pipe_operator" {
  for_each          = snowflake_pipe.silver
  account_role_name = var.role
  privileges        = ["MONITOR", "OPERATE"]
  on_schema_object {
    object_type = "PIPE"
    object_name = each.value.fully_qualified_name
  }

  lifecycle {
    replace_triggered_by = [snowflake_pipe.silver[each.key]]
  }
}

# LOADER writes SILVER only through the pipes it owns; no user is granted LOADER
resource "snowflake_grant_ownership" "pipe" {
  for_each            = snowflake_pipe.silver
  account_role_name   = snowflake_account_role.this["LOADER"].name
  outbound_privileges = "COPY"
  on {
    object_type = "PIPE"
    object_name = each.value.fully_qualified_name
  }
  depends_on = [
    snowflake_grant_account_role.to_sysadmin,
    snowflake_grant_privileges_to_account_role.pipe_operator,
    snowflake_grant_privileges_to_account_role.loader_tables,
    snowflake_grant_privileges_to_account_role.loader_stage,
    snowflake_grant_privileges_to_account_role.schema_usage,
    snowflake_grant_privileges_to_account_role.database_usage,
  ]

  lifecycle {
    replace_triggered_by = [snowflake_pipe.silver[each.key]]
  }
}
