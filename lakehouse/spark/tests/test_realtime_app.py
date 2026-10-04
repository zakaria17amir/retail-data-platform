from datetime import datetime
from typing import Any

import pytest
from lakehouse_spark.realtime.app import missing_tables, read_events
from pyspark.sql import DataFrame, SparkSession

pytestmark = pytest.mark.spark

BRONZE_SCHEMA = (
    "event_id string, event_type string, session_id string, event_ts string, product_id string, "
    "ingest_date date"
)


def _bronze(spark: SparkSession, root: str, event_type: str, rows: list[tuple[Any, ...]]) -> None:
    spark.createDataFrame(rows, BRONZE_SCHEMA).write.format("delta").save(
        f"{root}/bronze/events/{event_type}"
    )


def test_missing_tables_lists_types_without_a_bronze_table(spark: SparkSession, root: str) -> None:
    _bronze(spark, root, "page_view", [])
    assert missing_tables(spark, root, ("page_view", "add_to_cart")) == ["add_to_cart"]


def test_read_events_unions_types_and_parses_dataset_time(spark: SparkSession, root: str) -> None:
    day = datetime(2017, 6, 1).date()
    _bronze(spark, root, "page_view", [("e1", "page_view", "s1", "2017-06-01T12:00:00", None, day)])
    _bronze(
        spark,
        root,
        "product_view",
        [
            ("e2", "product_view", "s1", "2017-06-01T12:01:00", "p1", day),
            ("e3", "product_view", "s1", "garbage", "p1", day),
        ],
    )
    seen: list[dict[str, Any]] = []

    def collect(batch: DataFrame, batch_id: int) -> None:
        seen.extend(r.asDict() for r in batch.collect())

    query = (
        read_events(spark, root, ("page_view", "product_view"))
        .writeStream.foreachBatch(collect)
        .option("checkpointLocation", f"{root}/_checkpoints/test")
        .start()
    )
    try:
        query.processAllAvailable()
    finally:
        query.stop()
    assert sorted(seen, key=lambda r: r["event_ts"]) == [
        {
            "event_type": "page_view",
            "session_id": "s1",
            "product_id": None,
            "event_ts": datetime(2017, 6, 1, 12, 0),
        },
        {
            "event_type": "product_view",
            "session_id": "s1",
            "product_id": "p1",
            "event_ts": datetime(2017, 6, 1, 12, 1),
        },
    ]
