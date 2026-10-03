from datetime import datetime

import pytest
from lakehouse_spark.silver.domain.sales import ORDERS_RULES, localise
from lakehouse_spark.silver.rules import apply_rules
from pyspark.sql import SparkSession

pytestmark = pytest.mark.spark

ORDER_SCHEMA = (
    "order_id string, order_purchase_timestamp timestamp, order_approved_at timestamp, "
    "order_delivered_carrier_date timestamp, order_delivered_customer_date timestamp, "
    "order_estimated_delivery_date timestamp"
)


def _localised(spark: SparkSession, value: datetime) -> tuple[datetime, datetime]:
    df = spark.createDataFrame([(value,)], "shipping_limit_date timestamp")
    out = localise(df, {"shipping_limit_date": "shipping_limit_ts"})
    assert out.columns == ["shipping_limit_ts_local", "shipping_limit_ts_utc"]
    assert dict(out.dtypes) == {
        "shipping_limit_ts_local": "timestamp_ntz",
        "shipping_limit_ts_utc": "timestamp",
    }
    row = out.collect()[0]
    return row.shipping_limit_ts_local, row.shipping_limit_ts_utc


def test_localise_dst_summer(spark: SparkSession) -> None:
    local, utc = _localised(spark, datetime(2017, 1, 15, 10, 0, 0))
    assert local == datetime(2017, 1, 15, 10, 0, 0)
    assert utc == datetime(2017, 1, 15, 12, 0, 0)


def test_localise_winter(spark: SparkSession) -> None:
    local, utc = _localised(spark, datetime(2017, 7, 1, 10, 0, 0))
    assert local == datetime(2017, 7, 1, 10, 0, 0)
    assert utc == datetime(2017, 7, 1, 13, 0, 0)


def test_orders_flags_only_when_both_present(spark: SparkSession) -> None:
    t = datetime
    df = spark.createDataFrame(
        [
            ("ok", t(2017, 7, 1, 10), t(2017, 7, 1, 11), t(2017, 7, 2), t(2017, 7, 5), None),
            ("inv", t(2017, 7, 5), t(2017, 7, 4), t(2017, 7, 3), t(2017, 7, 2), None),
            ("nulls", t(2017, 7, 5), None, None, None, None),
            ("partial", t(2017, 7, 5), None, t(2017, 7, 1), t(2017, 7, 2), None),
        ],
        ORDER_SCHEMA,
    )
    kept, _, metrics = apply_rules(df, ORDERS_RULES)

    assert [m.rule_id for m in metrics] == [
        "orders_null_purchase_ts",
        "ts_localise",
        "orders_timeline_flags",
    ]
    flags = [
        "flag_approved_before_purchase",
        "flag_carrier_before_approved",
        "flag_delivered_before_carrier",
        "flag_delivered_before_purchase",
    ]
    got = {r.order_id: tuple(r[f] for f in flags) for r in kept.collect()}
    assert got == {
        "ok": (False, False, False, False),
        "inv": (True, True, True, True),
        "nulls": (False, False, False, False),
        "partial": (False, False, False, True),
    }
    for prefix in (
        "order_purchase_ts",
        "order_approved_ts",
        "order_delivered_carrier_ts",
        "order_delivered_customer_ts",
        "order_estimated_delivery_ts",
    ):
        assert f"{prefix}_local" in kept.columns
        assert f"{prefix}_utc" in kept.columns
    assert "order_purchase_timestamp" not in kept.columns


def test_orders_null_purchase_rejected(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [
            ("a", datetime(2017, 7, 1), None, None, None, None),
            ("b", None, datetime(2017, 7, 1), None, None, None),
        ],
        ORDER_SCHEMA,
    )
    kept, rejected, metrics = apply_rules(df, ORDERS_RULES)

    assert [r.order_id for r in kept.collect()] == ["a"]
    rows = rejected.collect()
    assert [(r.rule_id, '"order_id":"b"' in r.record_json) for r in rows] == [
        ("orders_null_purchase_ts", True)
    ]
    assert rows[0].reason
    assert (metrics[0].rows_in, metrics[0].rows_rejected) == (2, 1)
