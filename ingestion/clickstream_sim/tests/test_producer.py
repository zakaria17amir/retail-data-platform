import calendar
import io
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import fastavro
from clickstream_sim.events import Event
from clickstream_sim.faults import Emission
from clickstream_sim.producer import EventProducer, topic_for

SCHEMAS = Path(__file__).resolve().parents[2] / "schemas" / "events"
EVENT_TYPES = ("page_view", "search", "product_view", "add_to_cart", "checkout_started")


class FakeKafka:
    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = []

    def produce(self, topic: str, **kwargs: Any) -> None:
        self.messages.append({"topic": topic, **kwargs})

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float | None = None) -> int:
        return 0


def _event(**overrides: Any) -> Event:
    fields: dict[str, Any] = {
        "event_id": "e-1",
        "event_type": "add_to_cart",
        "session_id": "s1",
        "customer_id": None,
        "device": "mobile",
        "referrer": "google",
        "event_ts": "2017-10-02T10:00:00",
        "product_id": "p1",
        "search_query": None,
        "quantity": 1,
        "order_id": None,
    }
    fields.update(overrides)
    return Event(**fields)


def _emission(event: Event, version: int = 1) -> Emission:
    return Emission(event, 0.0, False, version)


def _producer(name: str) -> tuple[EventProducer, FakeKafka]:
    kafka = FakeKafka()
    return EventProducer("unused:9092", f"mock://{name}", SCHEMAS, producer=kafka), kafka


def _decode(value: bytes, version: int) -> dict[str, Any]:
    schema = fastavro.parse_schema(
        json.loads((SCHEMAS / f"clickstream_event.v{version}.avsc").read_text())
    )
    assert value[0] == 0
    record = fastavro.schemaless_reader(io.BytesIO(value[5:]), schema, schema)
    assert isinstance(record, dict)
    return record


def _ms(year: int, month: int, day: int, hour: int = 0, minute: int = 0, second: int = 0) -> int:
    return calendar.timegm((year, month, day, hour, minute, second)) * 1000


def test_topic_for() -> None:
    assert topic_for("add_to_cart") == "events.add_to_cart"


def test_serialized_value_decodes_back_to_event_dict() -> None:
    producer, kafka = _producer("decode")
    event = _event()
    producer.produce(_emission(event))
    message = kafka.messages[0]
    assert message["topic"] == "events.add_to_cart"
    assert message["key"] == b"e-1"
    assert _decode(message["value"], 1) == event.to_dict(1)


def test_none_event_id_stays_none_key() -> None:
    producer, kafka = _producer("nokey")
    producer.produce(_emission(_event(event_id=None)))
    assert kafka.messages[0]["key"] is None


def test_v1_registered_for_every_subject_and_v2_lazily() -> None:
    producer, _ = _producer("versions")
    for event_type in EVENT_TYPES:
        assert producer.registry.get_versions(f"events.{event_type}-value") == [1]
    producer.produce(_emission(_event(utm_campaign="spring"), version=2))
    assert sorted(producer.registry.get_versions("events.add_to_cart-value")) == [1, 2]
    assert producer.registry.get_versions("events.page_view-value") == [1]


def test_v2_message_carries_utm_campaign() -> None:
    producer, kafka = _producer("v2")
    event = _event(utm_campaign="spring")
    producer.produce(_emission(event, version=2))
    assert _decode(kafka.messages[0]["value"], 2) == event.to_dict(2)


def test_kafka_timestamp_is_event_ts_in_utc_ms() -> None:
    producer, kafka = _producer("ts")
    producer.produce(_emission(_event(event_ts="2017-10-02T10:00:00")))
    assert kafka.messages[0]["timestamp"] == _ms(2017, 10, 2, 10)


def test_implausible_timestamp_falls_back_to_dataset_time() -> None:
    producer, kafka = _producer("fallback")
    fallback = datetime(2017, 10, 2, 10, 30)
    for bad in ("not-a-timestamp", "1970-01-01T00:00:00", "2099-12-31T00:00:00"):
        producer.produce(_emission(_event(event_ts=bad)), fallback)
    assert [m["timestamp"] for m in kafka.messages] == [_ms(2017, 10, 2, 10, 30)] * 3


def test_unparseable_timestamp_without_fallback_reuses_last_good_timestamp() -> None:
    producer, kafka = _producer("lastgood")
    producer.produce(_emission(_event(event_ts="2017-10-02T10:00:00")))
    producer.produce(_emission(_event(event_ts="not-a-timestamp")))
    assert kafka.messages[1]["timestamp"] == _ms(2017, 10, 2, 10)


def test_flush_returns_remaining_messages() -> None:
    producer, _ = _producer("flush")
    assert producer.flush() == 0
