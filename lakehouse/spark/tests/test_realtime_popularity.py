from datetime import datetime
from typing import Any

import pytest
from lakehouse_spark.realtime.popularity import product_popularity
from lakehouse_spark.realtime.push import latest_per_key
from pyspark.sql import DataFrame, SparkSession

pytestmark = pytest.mark.spark

SCHEMA = "event_type string, product_id string, event_ts timestamp"


def _ts(hour: int, minute: int = 0) -> datetime:
    return datetime(2017, 6, 1, hour, minute)


def _popularity(df: DataFrame) -> dict[str, dict[str, Any]]:
    out = latest_per_key(product_popularity(df), "product_id")
    return {r["product_id"]: r.asDict() for r in out.collect()}


def test_popularity_counts_last_hour_and_last_24_hours(spark: SparkSession) -> None:
    rows = [
        ("product_view", "p1", _ts(10, 5)),
        ("product_view", "p1", _ts(12, 10)),
        ("add_to_cart", "p1", _ts(12, 20)),
        ("product_view", "p1", datetime(2017, 5, 31, 13, 0)),
        ("product_view", "p1", datetime(2017, 5, 31, 12, 59)),
        ("page_view", None, _ts(12, 25)),
        ("search", "p1", _ts(12, 26)),
        ("product_view", "p2", _ts(12, 30)),
    ]
    out = _popularity(spark.createDataFrame(rows, SCHEMA))
    assert out == {
        "p1": {
            "product_id": "p1",
            "views_1h": 1,
            "views_24h": 3,
            "carts_24h": 1,
            "event_ts": _ts(12, 20),
        },
        "p2": {
            "product_id": "p2",
            "views_1h": 1,
            "views_24h": 1,
            "carts_24h": 0,
            "event_ts": _ts(12, 30),
        },
    }


def test_late_event_inside_watermark_updates_popularity(spark: SparkSession, root: str) -> None:
    path = f"{root}/bronze_like"
    append = spark.createDataFrame([("product_view", "p1", _ts(12, 10))], SCHEMA)
    append.write.format("delta").save(path)
    outputs: list[dict[str, dict[str, Any]]] = []

    def collect(batch: DataFrame, batch_id: int) -> None:
        outputs.append(
            {r["product_id"]: r.asDict() for r in latest_per_key(batch, "product_id").collect()}
        )

    query = (
        product_popularity(spark.readStream.format("delta").load(path))
        .writeStream.outputMode("update")
        .foreachBatch(collect)
        .option("checkpointLocation", f"{root}/_checkpoints/realtime/product_popularity")
        .start()
    )
    try:
        query.processAllAvailable()
        late = [("product_view", "p1", _ts(12, 40)), ("product_view", "p1", _ts(11, 30))]
        spark.createDataFrame(late, SCHEMA).write.format("delta").mode("append").save(path)
        query.processAllAvailable()
        too_late = [("product_view", "p1", datetime(2017, 5, 30, 10, 0))]
        spark.createDataFrame(too_late, SCHEMA).write.format("delta").mode("append").save(path)
        query.processAllAvailable()
    finally:
        query.stop()
    batches = [o for o in outputs if o]
    assert [b["p1"]["views_24h"] for b in batches] == [1, 3]
    assert batches[-1]["p1"]["views_1h"] == 2
    assert batches[-1]["p1"]["event_ts"] == _ts(12, 40)
