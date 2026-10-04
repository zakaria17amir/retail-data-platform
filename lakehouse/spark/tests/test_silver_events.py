from datetime import datetime
from typing import Any

import pytest
from lakehouse_spark.silver.domain.events import event_rules
from lakehouse_spark.silver.rules import apply_rules
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

pytestmark = pytest.mark.spark

EVENT_SCHEMA = (
    "event_id string, event_type string, session_id string, customer_id string, device string, "
    "referrer string, event_ts string, product_id string, search_query string, quantity int, "
    "order_id string, utm_campaign string, _bronze_ingest_ts timestamp, kafka_partition int, "
    "kafka_offset long, rank int, rec_model_version string, rec_strategy string"
)
INGEST = datetime(2017, 7, 1, 12, 0, 0)


def _event(
    event_id: str,
    event_ts: str | None = "2017-07-01T10:00:00",
    session_id: str = "s1",
    quantity: int | None = None,
    ingest: datetime = INGEST,
    offset: int = 0,
    partition: int = 0,
    event_type: str = "page_view",
    rank: int | None = None,
) -> tuple[Any, ...]:
    return (
        event_id,
        event_type,
        session_id,
        "c1",
        "web",
        None,
        event_ts,
        None,
        None,
        quantity,
        None,
        None,
        ingest,
        partition,
        offset,
        rank,
        None if rank is None else "v1",
        None if rank is None else "rerank",
    )


def _events(spark: SparkSession, rows: list[tuple[Any, ...]]) -> DataFrame:
    return spark.createDataFrame(rows, EVENT_SCHEMA)


def _rejects(rejected: DataFrame) -> list[tuple[str, str]]:
    rows = rejected.select(
        "rule_id", F.get_json_object("record_json", "$.event_id").alias("event_id")
    ).collect()
    return sorted((r.rule_id, r.event_id) for r in rows)


def test_event_cast_failed(spark: SparkSession) -> None:
    df = _events(
        spark,
        [
            _event("ok", "2017-01-15T10:00:00"),
            _event("space", "2017-01-15 10:00:00"),
            _event("garbage", "not-a-date"),
            _event("null", None),
        ],
    )
    kept, rejected, _ = apply_rules(df, event_rules(None))

    assert _rejects(rejected) == [
        ("event_cast_failed", "garbage"),
        ("event_cast_failed", "null"),
        ("event_cast_failed", "space"),
    ]
    row = kept.collect()[0]
    assert row.event_id == "ok"
    assert row.event_ts_local == datetime(2017, 1, 15, 10, 0, 0)
    assert row.event_ts_utc == datetime(2017, 1, 15, 12, 0, 0)
    assert str(row.event_date) == "2017-01-15"
    assert "event_ts" not in kept.columns
    assert dict(kept.dtypes)["event_ts_local"] == "timestamp_ntz"


def test_event_negative_quantity(spark: SparkSession) -> None:
    df = _events(spark, [_event("neg", quantity=-1), _event("zero", quantity=0), _event("none")])
    kept, rejected, _ = apply_rules(df, event_rules(None))

    assert _rejects(rejected) == [("event_negative_quantity", "neg")]
    assert sorted(r.event_id for r in kept.collect()) == ["none", "zero"]


def test_feedback_missing_rank(spark: SparkSession) -> None:
    df = _events(
        spark,
        [
            _event("shown_no_rank", event_type="recommendation_shown"),
            _event("clicked_no_rank", event_type="recommendation_clicked"),
            _event("shown", event_type="recommendation_shown", rank=1),
            _event("clicked", event_type="recommendation_clicked", rank=3),
            _event("view_no_rank"),
        ],
    )
    kept, rejected, _ = apply_rules(df, event_rules(None))

    assert _rejects(rejected) == [
        ("feedback_missing_rank", "clicked_no_rank"),
        ("feedback_missing_rank", "shown_no_rank"),
    ]
    got = {r.event_id: (r.rank, r.rec_model_version, r.rec_strategy) for r in kept.collect()}
    assert got == {
        "shown": (1, "v1", "rerank"),
        "clicked": (3, "v1", "rerank"),
        "view_no_rank": (None, None, None),
    }


def test_event_duplicate_within_batch_and_vs_existing(spark: SparkSession) -> None:
    later = datetime(2017, 7, 1, 13, 0, 0)
    df = _events(
        spark,
        [
            _event("e1", ingest=later, offset=1, session_id="first"),
            _event("e1", ingest=INGEST, offset=9, session_id="second"),
            _event("e2", ingest=INGEST, offset=5, session_id="later_offset"),
            _event("e2", ingest=INGEST, offset=4, session_id="earlier_offset"),
            _event("e3", ingest=INGEST, offset=0, partition=1, session_id="later_partition"),
            _event("e3", ingest=INGEST, offset=7, partition=0, session_id="earlier_partition"),
            _event("old"),
            _event("new"),
        ],
    )
    existing = spark.createDataFrame(
        [("old", "s1", datetime(2017, 7, 1, 9, 0, 0))],
        "event_id string, session_id string, event_ts_local timestamp_ntz",
    )
    kept, rejected, _ = apply_rules(df, event_rules(existing))

    assert _rejects(rejected) == [
        ("event_duplicate", "e1"),
        ("event_duplicate", "e2"),
        ("event_duplicate", "e3"),
        ("event_duplicate", "old"),
    ]
    got = {r.event_id: r.session_id for r in kept.collect()}
    assert got == {
        "e1": "second",
        "e2": "earlier_offset",
        "e3": "earlier_partition",
        "new": "s1",
    }


def test_session_over_24h_uses_existing_start(spark: SparkSession) -> None:
    df = _events(
        spark,
        [
            _event("in_window", "2017-07-02T09:00:00", session_id="s1"),
            _event("exactly_24h", "2017-07-02T08:00:00", session_id="s2"),
            _event("over", "2017-07-02T09:00:01", session_id="s2"),
            _event("batch_only_start", "2017-07-01T08:00:00", session_id="s3"),
            _event("batch_only_late", "2017-07-02T08:30:00", session_id="s3"),
        ],
    )
    existing = spark.createDataFrame(
        [("prev", "s2", datetime(2017, 7, 1, 8, 0, 0))],
        "event_id string, session_id string, event_ts_local timestamp_ntz",
    )
    kept, rejected, metrics = apply_rules(df, event_rules(existing))

    assert _rejects(rejected) == [
        ("session_over_24h", "batch_only_late"),
        ("session_over_24h", "over"),
    ]
    assert sorted(r.event_id for r in kept.collect()) == [
        "batch_only_start",
        "exactly_24h",
        "in_window",
    ]
    assert [m.rule_id for m in metrics] == [
        "event_cast_failed",
        "ts_localise",
        "event_negative_quantity",
        "feedback_missing_rank",
        "event_duplicate",
        "session_over_24h",
    ]
