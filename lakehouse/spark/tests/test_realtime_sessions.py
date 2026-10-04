import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import datetime
from typing import Any

import pytest
from lakehouse_spark.realtime.app import load_categories, refresh_categories, with_categories
from lakehouse_spark.realtime.sessions import session_features
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming.query import StreamingQuery

pytestmark = pytest.mark.spark

SCHEMA = (
    "event_type string, session_id string, product_id string, category string, event_ts timestamp"
)
Event = tuple[str, str, str | None, str | None, datetime]


def _ts(hour: int, minute: int = 0) -> datetime:
    return datetime(2017, 6, 1, hour, minute)


class Stream:
    def __init__(self, spark: SparkSession, root: str) -> None:
        self.spark, self.path, self.root = spark, f"{root}/events", root
        self.batches: list[list[dict[str, Any]]] = []

    def append(self, rows: Sequence[Event]) -> None:
        df = self.spark.createDataFrame(list(rows), SCHEMA)
        df.write.format("delta").mode("append").save(self.path)

    def collect(self, batch: DataFrame, batch_id: int) -> None:
        rows = [r.asDict() for r in batch.collect()]
        if rows:
            self.batches.append(sorted(rows, key=lambda r: (r["session_id"], r["event_ts"])))

    @contextmanager
    def sessions(self) -> Iterator[StreamingQuery]:
        query = (
            session_features(self.spark.readStream.format("delta").load(self.path))
            .writeStream.outputMode("update")
            .foreachBatch(self.collect)
            .option("checkpointLocation", f"{self.root}/_checkpoints/realtime/session_features")
            .start()
        )
        try:
            yield query
        finally:
            query.stop()


def _feature(
    session_id: str, start: datetime, end: datetime, n_events: int, **overrides: Any
) -> dict[str, Any]:
    row = {
        "session_id": session_id,
        "n_events": n_events,
        "n_product_views": 0,
        "n_categories": 0,
        "last_category": None,
        "last_product_ids": None,
        "n_cart_adds": 0,
        "dwell_seconds": int((end - start).total_seconds()),
        "session_start_ts": start,
        "event_ts": end,
    }
    return row | overrides


def test_session_features_values_per_contract(spark: SparkSession, root: str) -> None:
    stream = Stream(spark, root)
    stream.append(
        [
            ("page_view", "s1", None, None, _ts(12, 0)),
            ("product_view", "s1", "p1", "A", _ts(12, 5)),
            ("product_view", "s1", "p2", "B", _ts(12, 10)),
            ("add_to_cart", "s1", "p2", "B", _ts(12, 12)),
            ("product_view", "s1", "p1", "A", _ts(12, 20)),
            ("search", "s1", None, None, _ts(12, 21)),
            ("page_view", "s2", None, None, _ts(12, 30)),
            *[("product_view", "s3", f"p{i}", None, _ts(13, i)) for i in range(1, 8)],
        ]
    )
    with stream.sessions() as query:
        query.processAllAvailable()
    assert stream.batches == [
        [
            _feature(
                "s1",
                _ts(12, 0),
                _ts(12, 21),
                6,
                n_product_views=3,
                n_categories=2,
                last_category="A",
                last_product_ids="p1,p2",
                n_cart_adds=1,
            ),
            _feature("s2", _ts(12, 30), _ts(12, 30), 1),
            _feature(
                "s3",
                _ts(13, 1),
                _ts(13, 7),
                7,
                n_product_views=7,
                last_product_ids="p7,p6,p5,p4,p3",
            ),
        ]
    ]


def test_30_minute_gap_splits_the_session(spark: SparkSession, root: str) -> None:
    stream = Stream(spark, root)
    stream.append(
        [
            ("page_view", "s1", None, None, _ts(12, 0)),
            ("page_view", "s1", None, None, _ts(12, 29)),
            ("page_view", "s1", None, None, _ts(12, 58)),
            ("page_view", "s1", None, None, _ts(13, 28)),
        ]
    )
    with stream.sessions() as query:
        query.processAllAvailable()
    # session_window semantics: an event exactly one gap after the last one opens a new session
    assert stream.batches == [
        [
            _feature("s1", _ts(12, 0), _ts(12, 58), 3),
            _feature("s1", _ts(13, 28), _ts(13, 28), 1),
        ]
    ]


def test_late_event_inside_watermark_updates_its_session(spark: SparkSession, root: str) -> None:
    stream = Stream(spark, root)
    stream.append(
        [
            ("page_view", "s1", None, None, _ts(12, 0)),
            ("page_view", "s1", None, None, _ts(12, 10)),
            ("page_view", "s2", None, None, _ts(14, 0)),
        ]
    )
    with stream.sessions() as query:
        query.processAllAvailable()
        stream.append([("page_view", "s1", None, None, _ts(12, 20))])
        query.processAllAvailable()
        stream.append([("page_view", "s1", None, None, _ts(11, 0))])
        query.processAllAvailable()
    assert stream.batches[1:] == [[_feature("s1", _ts(12, 0), _ts(12, 20), 3)]]


def test_timeout_closes_the_session(spark: SparkSession, root: str) -> None:
    stream = Stream(spark, root)
    stream.append([("page_view", "s1", None, None, _ts(12, 0))])
    with stream.sessions() as query:
        query.processAllAvailable()
        stream.append([("page_view", "s2", None, None, _ts(16, 0))])
        query.processAllAvailable()
        deadline = time.monotonic() + 30
        while _state_rows(query) != 1 and time.monotonic() < deadline:
            time.sleep(0.5)
        assert _state_rows(query) == 1
        stream.append([("page_view", "s1", None, None, _ts(15, 30))])
        query.processAllAvailable()
    assert stream.batches[-1] == [_feature("s1", _ts(15, 30), _ts(15, 30), 1)]


def _state_rows(query: StreamingQuery) -> int | None:
    progress = query.lastProgress
    return int(progress["stateOperators"][0]["numRowsTotal"]) if progress else None


def test_categories_come_from_silver_products_and_refresh(spark: SparkSession, root: str) -> None:
    products = f"{root}/silver/catalog/products"
    catalog = "product_id string, product_category_name string, is_current boolean"
    spark.createDataFrame([("p1", "A", True), ("p1", "old", False)], catalog).write.format(
        "delta"
    ).save(products)
    events_path = f"{root}/events"
    events = "product_id string, n int"
    spark.createDataFrame([("p1", 1), ("p2", 1)], events).write.format("delta").save(events_path)
    categories = load_categories(spark, root)
    seen: list[tuple[int, str, str | None]] = []

    def collect(batch: DataFrame, batch_id: int) -> None:
        seen.extend((r.n, r.product_id, r.category) for r in batch.collect())

    query = (
        with_categories(spark.readStream.format("delta").load(events_path), categories)
        .writeStream.foreachBatch(collect)
        .option("checkpointLocation", f"{root}/_checkpoints/test")
        .start()
    )
    try:
        query.processAllAvailable()
        spark.createDataFrame([("p2", "B", True)], catalog).write.format("delta").mode(
            "append"
        ).save(products)
        refresh_categories(categories)
        spark.createDataFrame([("p2", 2)], events).write.format("delta").mode("append").save(
            events_path
        )
        query.processAllAvailable()
    finally:
        query.stop()
    assert sorted(seen) == [(1, "p1", "A"), (1, "p2", None), (2, "p2", "B")]
