import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from lakehouse_spark.bronze.app import log_watermark_drops
from lakehouse_spark.bronze.decode import dedupe_stream, with_dedupe_key
from pyspark.sql import DataFrame, Row, SparkSession

from tests.helpers import RAW_SCHEMA

pytestmark = pytest.mark.spark

T0 = datetime(2017, 6, 1, 12, 0, 0)


def test_with_dedupe_key_uses_key_or_offset_triple(spark: SparkSession) -> None:
    data = [
        (b"k1", None, "events.page_view", 0, 1, T0),
        (b"k1", None, "events.page_view", 0, 2, T0),
        (None, None, "events.page_view", 0, 3, T0),
        (None, None, "events.page_view", 1, 3, T0),
    ]
    keyed = with_dedupe_key(spark.createDataFrame(data, RAW_SCHEMA))
    keys = [r.dedupe_key for r in keyed.collect()]
    assert keys[0] == keys[1] == "k1"
    assert keys[2] == "events.page_view-0-3"
    assert keys[3] == "events.page_view-1-3"
    assert len(set(keys)) == 3


def _write(directory: Path, name: str, rows: list[tuple[str, datetime]]) -> None:
    lines = [
        json.dumps(
            {"key": key, "timestamp": ts.isoformat(), "topic": "t", "partition": 0, "offset": i}
        )
        for i, (key, ts) in enumerate(rows)
    ]
    (directory / name).write_text("\n".join(lines), encoding="utf-8")


def test_dedupe_stream_drops_duplicates_within_watermark(
    spark: SparkSession, tmp_path: Path
) -> None:
    source = tmp_path / "in"
    source.mkdir()
    survivors: list[Row] = []

    def collect(batch: DataFrame, batch_id: int) -> None:
        survivors.extend(batch.collect())

    stream = (
        spark.readStream.schema(
            "key string, timestamp timestamp, topic string, partition int, offset long"
        )
        .option("maxFilesPerTrigger", 1)
        .json(str(source))
    )
    query = (
        dedupe_stream(with_dedupe_key(stream))
        .writeStream.foreachBatch(collect)
        .option("checkpointLocation", str(tmp_path / "ckpt"))
        .start()
    )
    try:
        _write(source, "1.json", [("A", T0), ("A", T0 + timedelta(hours=1))])
        query.processAllAvailable()
        assert [(r.key, r.timestamp) for r in survivors] == [("A", T0)]

        _write(source, "2.json", [("B", T0 + timedelta(days=7))])
        query.processAllAvailable()
        _write(source, "3.json", [("A", T0 + timedelta(days=7, hours=1))])
        query.processAllAvailable()
        query.processAllAvailable()
    finally:
        query.stop()

    assert sorted(r.key for r in survivors) == ["A", "A", "B"]
    latest_a = max(r.timestamp for r in survivors if r.key == "A")
    assert latest_a == T0 + timedelta(days=7, hours=1)


def test_late_events_survive_and_drops_are_logged(
    spark: SparkSession, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    source = tmp_path / "in"
    source.mkdir()
    survivors: list[Row] = []

    def collect(batch: DataFrame, batch_id: int) -> None:
        survivors.extend(batch.collect())

    stream = (
        spark.readStream.schema(
            "key string, timestamp timestamp, topic string, partition int, offset long"
        )
        .option("maxFilesPerTrigger", 1)
        .json(str(source))
    )
    query = (
        dedupe_stream(with_dedupe_key(stream))
        .writeStream.queryName("events_to_bronze")
        .foreachBatch(collect)
        .option("checkpointLocation", str(tmp_path / "ckpt"))
        .start()
    )
    newest = T0 + timedelta(hours=80)
    try:
        _write(source, "1.json", [("NEW", newest)])
        query.processAllAvailable()
        _write(
            source,
            "2.json",
            [("L47", newest - timedelta(hours=47)), ("L50", newest - timedelta(hours=50))],
        )
        query.processAllAvailable()
        _write(source, "3.json", [("L80", newest - timedelta(hours=80))])
        query.processAllAvailable()
        progress = [p.json for p in query.recentProgress]
    finally:
        query.stop()

    assert sorted(r.key for r in survivors) == ["L47", "L50", "NEW"]
    with caplog.at_level(logging.WARNING, logger="bronze"):
        assert sum(log_watermark_drops(p) for p in progress) == 1
    assert "events_to_bronze dropped 1 rows behind the watermark" in caplog.text
