from datetime import datetime, timedelta
from typing import Any

import pytest
from lakehouse_spark.realtime.popularity import product_popularity
from lakehouse_spark.realtime.push import latest_per_key
from pyspark.sql import DataFrame, SparkSession

pytestmark = pytest.mark.spark

SCHEMA = "event_type string, product_id string, event_ts timestamp"


def _ts(hour: int, minute: int = 0) -> datetime:
    return datetime(2017, 6, 1, hour, minute)


def _version(hour_start: datetime, events_24h: int) -> datetime:
    return hour_start + timedelta(microseconds=events_24h)


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
            "event_ts": _version(_ts(12), 4),
        },
        "p2": {
            "product_id": "p2",
            "views_1h": 1,
            "views_24h": 1,
            "carts_24h": 0,
            "event_ts": _version(_ts(12), 1),
        },
    }


class Stream:
    def __init__(self, spark: SparkSession, root: str) -> None:
        self.spark, self.root, self.path = spark, root, f"{root}/bronze_like"
        self.outputs: list[dict[str, dict[str, Any]]] = []

    def append(self, rows: list[tuple[str, str, datetime]]) -> None:
        df = self.spark.createDataFrame(rows, SCHEMA)
        df.write.format("delta").mode("append").save(self.path)

    def run(self, *appends: list[tuple[str, str, datetime]]) -> list[dict[str, dict[str, Any]]]:
        def collect(batch: DataFrame, batch_id: int) -> None:
            self.outputs.append(
                {r["product_id"]: r.asDict() for r in latest_per_key(batch, "product_id").collect()}
            )

        self.append(appends[0])
        query = (
            product_popularity(self.spark.readStream.format("delta").load(self.path))
            .writeStream.outputMode("update")
            .foreachBatch(collect)
            .option("checkpointLocation", f"{self.root}/_checkpoints/realtime/product_popularity")
            .start()
        )
        try:
            query.processAllAvailable()
            for rows in appends[1:]:
                self.append(rows)
                query.processAllAvailable()
        finally:
            query.stop()
        return [o for o in self.outputs if o]


def test_late_event_inside_watermark_updates_popularity(spark: SparkSession, root: str) -> None:
    batches = Stream(spark, root).run(
        [("product_view", "p1", _ts(12, 10))],
        [("product_view", "p1", _ts(12, 40)), ("product_view", "p1", _ts(11, 30))],
        # watermark = 12:40 - 49 h = May 30 11:40
        [("product_view", "p1", datetime(2017, 5, 30, 10, 0))],
    )
    assert [b["p1"]["views_24h"] for b in batches] == [1, 3]
    assert batches[-1]["p1"]["views_1h"] == 2
    assert batches[-1]["p1"]["event_ts"] == _version(_ts(12), 3)


def test_every_update_pushes_a_strictly_newer_event_ts(spark: SparkSession, root: str) -> None:
    # Feast's Redis store skips a write whose timestamp is not newer than the stored row: a cart
    # committed after a later view (one bronze table per type) must still move the version
    batches = Stream(spark, root).run(
        [("product_view", "p1", _ts(12, 40))],
        [("add_to_cart", "p1", _ts(12, 20))],
        [("product_view", "p1", _ts(12, 40))],
        [("product_view", "p1", _ts(13, 5))],
    )
    rows = [b["p1"] for b in batches]
    assert [(r["views_24h"], r["carts_24h"]) for r in rows] == [(1, 0), (1, 1), (2, 1), (3, 1)]
    stamps = [r["event_ts"] for r in rows]
    assert stamps == sorted(set(stamps))
    assert stamps[-1] == _version(_ts(13), 4)
