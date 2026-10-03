from collections.abc import Mapping

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from lakehouse_spark.silver.rules import Rule, transform

LOCAL_TZ = "America/Sao_Paulo"


def localise(df: DataFrame, columns: Mapping[str, str]) -> DataFrame:
    for source, prefix in columns.items():
        local = F.col(source).cast("timestamp_ntz")
        df = df.withColumn(f"{prefix}_local", local).withColumn(
            f"{prefix}_utc", F.to_utc_timestamp(local, LOCAL_TZ)
        )
    return df.drop(*columns)


def _null_purchase(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    missing = F.col("order_purchase_timestamp").isNull()
    return df.filter(~missing), df.filter(missing).withColumn(
        "reason", F.lit("order_purchase_timestamp is null")
    )


def _before(later: str, earlier: str) -> Column:
    return F.coalesce(F.col(f"{later}_utc") < F.col(f"{earlier}_utc"), F.lit(False))


def _timeline_flags(df: DataFrame) -> DataFrame:
    return df.withColumns(
        {
            "flag_approved_before_purchase": _before("order_approved_ts", "order_purchase_ts"),
            "flag_carrier_before_approved": _before(
                "order_delivered_carrier_ts", "order_approved_ts"
            ),
            "flag_delivered_before_carrier": _before(
                "order_delivered_customer_ts", "order_delivered_carrier_ts"
            ),
            "flag_delivered_before_purchase": _before(
                "order_delivered_customer_ts", "order_purchase_ts"
            ),
        }
    )


def _localise_rule(columns: Mapping[str, str]) -> Rule:
    return transform(
        "ts_localise",
        "São Paulo wall clock → <x>_local (ntz) and <x>_utc",
        lambda df: localise(df, columns),
    )


ORDERS_RULES: tuple[Rule, ...] = (
    Rule("orders_null_purchase_ts", "order without purchase timestamp", _null_purchase),
    _localise_rule(
        {
            "order_purchase_timestamp": "order_purchase_ts",
            "order_approved_at": "order_approved_ts",
            "order_delivered_carrier_date": "order_delivered_carrier_ts",
            "order_delivered_customer_date": "order_delivered_customer_ts",
            "order_estimated_delivery_date": "order_estimated_delivery_ts",
        }
    ),
    transform(
        "orders_timeline_flags",
        "flag inverted order timelines where both timestamps are present",
        _timeline_flags,
    ),
)
ORDER_ITEMS_RULES: tuple[Rule, ...] = (
    _localise_rule({"shipping_limit_date": "shipping_limit_ts"}),
)
PAYMENTS_RULES: tuple[Rule, ...] = ()
REVIEWS_RULES: tuple[Rule, ...] = (
    _localise_rule(
        {
            "review_creation_date": "review_creation_ts",
            "review_answer_timestamp": "review_answer_ts",
        }
    ),
)
