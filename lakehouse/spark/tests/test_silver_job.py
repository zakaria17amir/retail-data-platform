from datetime import date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pytest
from delta.tables import DeltaTable
from lakehouse_spark.silver import job
from lakehouse_spark.silver.job import run_table
from lakehouse_spark.silver.rules import Rule
from lakehouse_spark.silver.tables import TABLES, TableSpec
from pyspark.errors import StreamingQueryException
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F

from tests.helpers import bronze, cdc, event

pytestmark = pytest.mark.spark

INGEST_TS = datetime(2017, 6, 1, 12, 5, 0)
EVENT_COLUMNS = [
    "event_id",
    "event_type",
    "session_id",
    "customer_id",
    "device",
    "referrer",
    "event_ts_local",
    "event_ts_utc",
    "product_id",
    "search_query",
    "quantity",
    "order_id",
    "utm_campaign",
    "event_date",
    "_bronze_ingest_ts",
    "_silver_loaded_at",
    "_run_id",
]
EVENT_BRONZE = (
    "event_id string, event_type string, session_id string, customer_id string, device string, "
    "referrer string, event_ts string, product_id string, search_query string, quantity int, "
    "order_id string, kafka_topic string, kafka_partition int, kafka_offset long, "
    "kafka_timestamp timestamp, schema_id long, ingest_ts timestamp, ingest_date date"
)


def _bad_name(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    bad = F.col("name") == "bad"
    return df.filter(~bad), df.filter(bad).withColumn("reason", F.lit("name is bad"))


THINGS = TableSpec(
    name="test/things",
    bronze=("olist/things",),
    mode="current",
    key=("id",),
    rules=lambda spark, silver: (Rule("test_bad_name", "bad name", _bad_name),),
)
PEOPLE = TableSpec(
    name="test/people",
    bronze=("olist/people",),
    mode="scd2",
    key=("id",),
    tracked=("name",),
    rules=lambda spark, silver: (),
)
EVENTS = next(spec for spec in TABLES if spec.name == "events/clickstream")


def _append(spark: SparkSession, root: str, rel: str, df: DataFrame) -> None:
    df.write.format("delta").mode("append").option("mergeSchema", "true").save(
        f"{root}/bronze/{rel}"
    )


def _silver(spark: SparkSession, root: str, rel: str) -> DataFrame:
    return spark.read.format("delta").load(f"{root}/silver/{rel}")


def _version(spark: SparkSession, root: str, rel: str) -> int:
    return int(DeltaTable.forPath(spark, f"{root}/silver/{rel}").history(1).collect()[0]["version"])


def _metrics(spark: SparkSession, root: str, run_id: str) -> list[tuple[Any, ...]]:
    rows = _silver(spark, root, "_rule_metrics").filter(F.col("run_id") == run_id).collect()
    return [(r.table, r.rule_id, r.rows_in, r.rows_rejected) for r in rows if r.rows_in > 0]


def _events(spark: SparkSession, rows: list[tuple[str, int]], **extra: Any) -> DataFrame:
    data = [
        (
            *event(event_id=event_id, **extra).values(),
            "events.page_view",
            0,
            offset,
            INGEST_TS,
            1,
            INGEST_TS,
            date(2017, 6, 1),
        )
        for event_id, offset in rows
    ]
    schema = EVENT_BRONZE
    if extra:
        schema = schema.replace(
            ", kafka_topic", "".join(f", {k} string" for k in extra) + ", kafka_topic"
        )
    return spark.createDataFrame(data, schema)


def _things(spark: SparkSession, root: str) -> None:
    rows = [
        cdc("r", "a", "x", 1, 0),
        cdc("r", "b", "bad", 2, 1),
        cdc("r", "a", "x", 1, 2),
        cdc("u", "a", "y", 3, 3),
    ]
    _append(spark, root, "olist/things", bronze(spark, rows))


def test_run_table_current_end_to_end(spark: SparkSession, root: str) -> None:
    _things(spark, root)
    run_table(spark, THINGS, root, "r1")

    silver = _silver(spark, root, "test/things")
    assert "_op" not in silver.columns and "_kafka_offset" not in silver.columns
    rows = silver.collect()
    assert [(r.id, r.name, r._source_lsn, r._is_deleted, r._run_id) for r in rows] == [
        ("a", "y", 3, False, "r1")
    ]
    assert rows[0]._silver_loaded_at is not None

    rejects = _silver(spark, root, "_rejects/test/things").collect()
    assert sorted((r.rule_id, r.reason, r._run_id) for r in rejects) == [
        ("cdc_exact_duplicate", "duplicate delivery of key+lsn", "r1"),
        ("test_bad_name", "name is bad", "r1"),
    ]
    assert all(r._rejected_at is not None for r in rejects)
    assert '"id":"b"' in next(r.record_json for r in rejects if r.rule_id == "test_bad_name")

    assert _metrics(spark, root, "r1") == [
        ("test/things", "cdc_exact_duplicate", 4, 1),
        ("test/things", "cdc_collapse", 3, 0),
        ("test/things", "test_bad_name", 2, 1),
    ]
    pct = _silver(spark, root, "_rule_metrics").filter("rule_id = 'cdc_exact_duplicate'")
    assert pct.collect()[0]["pct_rejected"] == 25.0


def _snapshot(spark: SparkSession, root: str) -> tuple[Any, ...]:
    tables = ("test/things", "_rejects/test/things", "_rule_metrics")
    rows = sorted(tuple(r) for r in _silver(spark, root, "test/things").collect())
    return rows, [_version(spark, root, t) for t in tables]


def test_run_table_rerun_noop(spark: SparkSession, root: str) -> None:
    _things(spark, root)
    run_table(spark, THINGS, root, "r1")
    once = _snapshot(spark, root)

    run_table(spark, THINGS, root, "r2")
    assert _snapshot(spark, root) == once
    assert _metrics(spark, root, "r2") == []

    # crash after the writes, before the checkpoint commit: the same batch is processed again
    commits = Path(urlparse(root).path) / "_checkpoints" / "silver" / "test_things" / "commits"
    for name in ("0", ".0.crc"):
        (commits / name).unlink(missing_ok=True)
    run_table(spark, THINGS, root, "r3")
    assert _snapshot(spark, root) == once
    assert _metrics(spark, root, "r3") == []


def _versions(spark: SparkSession, root: str) -> list[Row]:
    return sorted(_silver(spark, root, "test/people").collect(), key=lambda r: r._source_lsn)


def test_run_table_scd2(spark: SparkSession, root: str) -> None:
    _append(spark, root, "olist/people", bronze(spark, [cdc("r", "a", "x", 1, 0)]))
    run_table(spark, PEOPLE, root, "r1")
    later = [cdc("u", "a", "y", 2, 1), cdc("u", "a", "y", 3, 2, updated=1)]
    _append(spark, root, "olist/people", bronze(spark, later))
    run_table(spark, PEOPLE, root, "r2")

    versions = _versions(spark, root)
    assert [(r.name, r._source_lsn, r._run_id, r.is_current) for r in versions] == [
        ("x", 1, "r1", False),
        ("y", 2, "r2", True),
    ]
    assert versions[0].valid_to == versions[1].valid_from
    assert all(r._silver_loaded_at is not None for r in versions)


def test_run_table_events_dedupes_across_runs(spark: SparkSession, root: str) -> None:
    _append(spark, root, "events/page_view", _events(spark, [("e1", 0), ("e2", 1), ("e1", 2)]))
    run_table(spark, EVENTS, root, "r1")
    _append(spark, root, "events/page_view", _events(spark, [("e2", 3), ("e3", 4)]))
    run_table(spark, EVENTS, root, "r2")

    silver = _silver(spark, root, "events/clickstream")
    assert silver.columns == EVENT_COLUMNS
    rows = silver.collect()
    assert sorted((r.event_id, r._run_id) for r in rows) == [
        ("e1", "r1"),
        ("e2", "r1"),
        ("e3", "r2"),
    ]
    assert {(r._bronze_ingest_ts, r.utm_campaign) for r in rows} == {(INGEST_TS, None)}
    rejects = _silver(spark, root, "_rejects/events/clickstream").collect()
    assert sorted((r.rule_id, r.reason, r._run_id) for r in rejects) == [
        ("event_duplicate", "event_id already in silver", "r2"),
        ("event_duplicate", "event_id seen earlier in batch", "r1"),
    ]
    assert all('"kafka_offset"' in r.record_json for r in rejects)


def test_schema_change_restart(spark: SparkSession, root: str) -> None:
    _append(spark, root, "events/page_view", _events(spark, [("e1", 0)]))
    run_table(spark, EVENTS, root, "r1")
    v2 = _events(spark, [("e2", 1)], utm_campaign="summer")
    _append(spark, root, "events/page_view", v2)
    run_table(spark, EVENTS, root, "r2")

    rows = _silver(spark, root, "events/clickstream").collect()
    assert sorted((r.event_id, r.utm_campaign) for r in rows) == [("e1", None), ("e2", "summer")]


def test_schema_change_restart_mid_run(
    spark: SparkSession, root: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Delta only raises when a schema change lands while the query runs; simulate that race
    _append(spark, root, "events/page_view", _events(spark, [("e1", 0)]))
    real, failures = job.process_batch, list[int]()

    def flaky(batch: DataFrame, batch_id: int, **kwargs: Any) -> int:
        if not failures:
            failures.append(batch_id)
            v2 = _events(spark, [("e2", 1)], utm_campaign="summer")
            _append(spark, root, "events/page_view", v2)
            raise RuntimeError("[DELTA_SCHEMA_CHANGED] simulated")
        return real(batch, batch_id, **kwargs)

    monkeypatch.setattr(job, "process_batch", flaky)
    run_table(spark, EVENTS, root, "r1")

    rows = _silver(spark, root, "events/clickstream").collect()
    assert sorted((r.event_id, r.utm_campaign) for r in rows) == [("e1", None), ("e2", "summer")]
    assert failures == [0]


def test_non_schema_errors_are_not_retried(
    spark: SparkSession, root: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    _append(spark, root, "events/page_view", _events(spark, [("e1", 0)]))
    calls: list[int] = []

    def broken(batch: DataFrame, batch_id: int, **kwargs: Any) -> int:
        calls.append(batch_id)
        raise RuntimeError("boom")

    monkeypatch.setattr(job, "process_batch", broken)
    with pytest.raises(StreamingQueryException):
        run_table(spark, EVENTS, root, "r1")
    assert calls == [0]
