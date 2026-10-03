import pytest
from lakehouse_spark.bronze.app import process_batch
from pyspark.sql import SparkSession

from tests.helpers import EVENT_V1, FakeRegistry, encode, event, raw_batch

pytestmark = pytest.mark.spark

REGISTRY = FakeRegistry({1: EVENT_V1})


def _count(spark: SparkSession, path: str) -> int:
    return spark.read.format("delta").load(path).count()


def _run(spark: SparkSession, root: str, batch_id: int) -> None:
    rows = [
        (encode(EVENT_V1, event(event_id="a"), 1), "events.page_view"),
        (b"junk-1", "events.page_view"),
        (encode(EVENT_V1, event(event_id="b", event_type="search"), 1), "events.search"),
        (b"junk-2", "events.search"),
    ]
    process_batch(
        raw_batch(spark, rows),
        batch_id,
        name="events_to_bronze",
        source="events",
        root=root,
        registry=REGISTRY,
    )


def test_process_batch_writes_all_topics_and_one_quarantine_txn(
    spark: SparkSession, root: str
) -> None:
    _run(spark, root, 0)
    assert _count(spark, f"{root}/bronze/events/page_view") == 1
    assert _count(spark, f"{root}/bronze/events/search") == 1
    quarantine = spark.read.format("delta").load(f"{root}/bronze/_quarantine/events")
    assert sorted(r.kafka_topic for r in quarantine.collect()) == [
        "events.page_view",
        "events.search",
    ]


def test_process_batch_is_idempotent_per_batch_id(spark: SparkSession, root: str) -> None:
    _run(spark, root, 0)
    _run(spark, root, 0)
    assert _count(spark, f"{root}/bronze/events/page_view") == 1
    assert _count(spark, f"{root}/bronze/events/search") == 1
    assert _count(spark, f"{root}/bronze/_quarantine/events") == 2

    _run(spark, root, 1)
    assert _count(spark, f"{root}/bronze/events/page_view") == 2
    assert _count(spark, f"{root}/bronze/events/search") == 2
    assert _count(spark, f"{root}/bronze/_quarantine/events") == 4
