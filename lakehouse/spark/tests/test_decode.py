from urllib.error import URLError

import pytest
from lakehouse_spark.bronze.decode import QUARANTINE_REASONS, decode_batch
from lakehouse_spark.bronze.sink import write_bronze, write_quarantine
from pyspark.sql import DataFrame, SparkSession

from tests.helpers import (
    CDC_ORDERS,
    EVENT_V1,
    EVENT_V2,
    BrokenRegistry,
    FakeRegistry,
    encode,
    event,
    raw_batch,
)

pytestmark = pytest.mark.spark

REGISTRY = FakeRegistry({1: EVENT_V1, 2: EVENT_V2, 3: CDC_ORDERS})
TOPIC = "events.page_view"


def _reasons(bad: DataFrame) -> list[str]:
    return sorted(r.reason for r in bad.select("reason").collect())


def _envelope(before: object, after: object, op: str) -> bytes:
    return encode(CDC_ORDERS, {"before": before, "after": after, "op": op, "ts_ms": 1}, 3)


def test_good_event_decoded_with_metadata(spark: SparkSession) -> None:
    batch = raw_batch(spark, [(encode(EVENT_V1, event(), 1), TOPIC)])
    good, bad = decode_batch(batch, REGISTRY, "events")
    row = good.collect()[0]
    assert row.event_id == "e1"
    assert row.event_ts == "2017-06-01T12:00:00"
    assert (row.kafka_topic, row.kafka_partition, row.kafka_offset, row.schema_id) == (
        TOPIC,
        0,
        0,
        1,
    )
    assert row.ingest_ts is not None
    assert row.ingest_date is not None
    assert bad.count() == 0


def test_two_schema_ids_union_adds_utm_column(spark: SparkSession) -> None:
    batch = raw_batch(
        spark,
        [
            (encode(EVENT_V1, event(event_id="e1"), 1), TOPIC),
            (encode(EVENT_V2, event(event_id="e2", utm_campaign="spring"), 2), TOPIC),
        ],
    )
    good, _ = decode_batch(batch, REGISTRY, "events")
    assert {r.event_id: r.utm_campaign for r in good.collect()} == {"e1": None, "e2": "spring"}


def test_null_value_quarantined_as_null_payload(spark: SparkSession) -> None:
    good, bad = decode_batch(raw_batch(spark, [(None, TOPIC)]), REGISTRY, "events")
    assert good.count() == 0
    assert _reasons(bad) == ["null_payload"]
    assert bad.collect()[0].raw_value is None


def test_plain_text_quarantined_as_not_wire_format(spark: SparkSession) -> None:
    good, bad = decode_batch(raw_batch(spark, [(b"hello world", TOPIC)]), REGISTRY, "events")
    assert good.count() == 0
    assert _reasons(bad) == ["not_wire_format"]
    row = bad.collect()[0]
    assert row.schema_id is None
    assert bytes(row.raw_value) == b"hello world"


def test_unknown_schema_id_quarantined(spark: SparkSession) -> None:
    batch = raw_batch(spark, [(encode(EVENT_V1, event(), 99), TOPIC)])
    good, bad = decode_batch(batch, REGISTRY, "events")
    assert good.count() == 0
    assert _reasons(bad) == ["unknown_schema_id"]
    assert bad.collect()[0].schema_id == 99


def test_registry_connection_error_propagates(spark: SparkSession) -> None:
    batch = raw_batch(spark, [(encode(EVENT_V1, event(), 1), TOPIC)])
    with pytest.raises(URLError):
        decode_batch(batch, BrokenRegistry(), "events")


def test_corrupt_avro_payload_quarantined(spark: SparkSession) -> None:
    batch = raw_batch(spark, [(b"\x00\x00\x00\x00\x01\xff\xff", TOPIC)])
    good, bad = decode_batch(batch, REGISTRY, "events")
    assert good.count() == 0
    assert _reasons(bad) == ["avro_decode_failed"]


@pytest.mark.parametrize("field", ["event_id", "session_id"])
def test_null_event_or_session_id_quarantined_null_primary_key(
    spark: SparkSession, field: str
) -> None:
    batch = raw_batch(spark, [(encode(EVENT_V1, event(**{field: None}), 1), TOPIC)])
    good, bad = decode_batch(batch, REGISTRY, "events")
    assert good.count() == 0
    assert _reasons(bad) == ["null_primary_key"]


@pytest.mark.parametrize(
    "event_ts", ["not-a-timestamp", "1970-01-01T00:00:00", "2099-12-31T00:00:00"]
)
def test_bad_timestamp_quarantined(spark: SparkSession, event_ts: str) -> None:
    batch = raw_batch(spark, [(encode(EVENT_V1, event(event_ts=event_ts), 1), TOPIC)])
    good, bad = decode_batch(batch, REGISTRY, "events")
    assert good.count() == 0
    assert _reasons(bad) == ["unparseable_timestamp"]
    assert bad.collect()[0].raw_value is not None


def test_cdc_null_before_and_after_quarantined(spark: SparkSession) -> None:
    topic = "cdc.olist.orders"
    rows = [
        (_envelope(None, None, "u"), topic),
        (_envelope(None, {"order_id": "o1"}, "c"), topic),
        (_envelope({"order_id": "o2"}, None, "d"), topic),
    ]
    good, bad = decode_batch(raw_batch(spark, rows), REGISTRY, "olist")
    assert sorted(r.op for r in good.collect()) == ["c", "d"]
    assert _reasons(bad) == ["null_payload"]


def test_counts_add_up(spark: SparkSession) -> None:
    rows = [
        (encode(EVENT_V1, event(event_id="a"), 1), TOPIC),
        (encode(EVENT_V2, event(event_id="b"), 2), TOPIC),
        (encode(EVENT_V1, event(event_id=None), 1), TOPIC),
        (encode(EVENT_V1, event(event_ts="nope"), 1), TOPIC),
        (encode(EVENT_V1, event(), 99), TOPIC),
        (b"text", TOPIC),
        (None, TOPIC),
    ]
    good, bad = decode_batch(raw_batch(spark, rows), REGISTRY, "events")
    assert good.count() == 2
    assert bad.count() == 5
    assert set(_reasons(bad)) <= set(QUARANTINE_REASONS)


def test_write_bronze_routes_by_topic_and_partitions_by_ingest_date(
    spark: SparkSession, root: str
) -> None:
    rows = [
        (encode(EVENT_V1, event(event_id="a"), 1), "events.page_view"),
        (encode(EVENT_V1, event(event_id="b", event_type="search"), 1), "events.search"),
    ]
    good, _ = decode_batch(raw_batch(spark, rows), REGISTRY, "events")
    write_bronze(good, root, "events")
    view = spark.read.format("delta").load(f"{root}/bronze/events/page_view")
    search = spark.read.format("delta").load(f"{root}/bronze/events/search")
    assert [r.event_id for r in view.collect()] == ["a"]
    assert [r.event_id for r in search.collect()] == ["b"]
    assert view.inputFiles()[0].split("/")[-2].startswith("ingest_date=")


def test_write_bronze_merge_schema(spark: SparkSession, root: str) -> None:
    first = raw_batch(spark, [(encode(EVENT_V1, event(event_id="a"), 1), TOPIC)])
    second = raw_batch(spark, [(encode(EVENT_V2, event(event_id="b", utm_campaign="x"), 2), TOPIC)])
    write_bronze(decode_batch(first, REGISTRY, "events")[0], root, "events")
    write_bronze(decode_batch(second, REGISTRY, "events")[0], root, "events")
    table = spark.read.format("delta").load(f"{root}/bronze/events/page_view")
    assert "utm_campaign" in table.columns
    assert {r.event_id: r.utm_campaign for r in table.collect()} == {"a": None, "b": "x"}


def test_write_quarantine_appends_under_source(spark: SparkSession, root: str) -> None:
    _, bad = decode_batch(raw_batch(spark, [(b"junk", TOPIC)]), REGISTRY, "events")
    write_quarantine(bad, root, "events")
    table = spark.read.format("delta").load(f"{root}/bronze/_quarantine/events")
    assert [r.reason for r in table.collect()] == ["not_wire_format"]
