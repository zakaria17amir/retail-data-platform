import calendar
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Protocol, cast

from confluent_kafka import Producer
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from clickstream_sim.events import EVENT_TYPES, TS_FORMAT
from clickstream_sim.faults import Emission

MAX_FALLBACK_SKEW = timedelta(days=3)
MIN_PLAUSIBLE_YEAR = 2016
MAX_PLAUSIBLE_YEAR = 2030
FLUSH_TIMEOUT_S = 30


class KafkaProducer(Protocol):
    def produce(self, topic: str, **kwargs: Any) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float = ...) -> int: ...


def topic_for(event_type: str) -> str:
    return f"events.{event_type}"


def _epoch_ms(moment: datetime) -> int:
    return calendar.timegm(moment.timetuple()) * 1000


def _parse(event_ts: str) -> datetime | None:
    try:
        return datetime.strptime(event_ts, TS_FORMAT)
    except ValueError:
        return None


class EventProducer:
    def __init__(
        self,
        bootstrap: str,
        registry_url: str,
        schemas_dir: Path,
        *,
        producer: KafkaProducer | None = None,
    ) -> None:
        self.registry = SchemaRegistryClient.new_client({"url": registry_url})
        self.errors = 0
        self._schemas = {
            version: (schemas_dir / f"clickstream_event.v{version}.avsc").read_text(
                encoding="utf-8"
            )
            for version in (1, 2)
        }
        self._serializers: dict[int, AvroSerializer] = {}
        self._producer: KafkaProducer = producer or cast(
            KafkaProducer,
            Producer({"bootstrap.servers": bootstrap, "linger.ms": 20, "enable.idempotence": True}),
        )
        self._last_ms: int | None = None
        for event_type in EVENT_TYPES:
            self.registry.register_schema(
                f"{topic_for(event_type)}-value", Schema(self._schemas[1], "AVRO")
            )

    def _serializer(self, version: int) -> AvroSerializer:
        if version not in self._serializers:
            self._serializers[version] = AvroSerializer(self.registry, self._schemas[version])
        return self._serializers[version]

    def _timestamp_ms(self, event_ts: str, fallback: datetime | None) -> int:
        parsed = _parse(event_ts)
        if fallback is not None:
            plausible = parsed is not None and abs(parsed - fallback) <= MAX_FALLBACK_SKEW
        else:
            plausible = (
                parsed is not None and MIN_PLAUSIBLE_YEAR <= parsed.year <= MAX_PLAUSIBLE_YEAR
            )
        if plausible and parsed is not None:
            self._last_ms = _epoch_ms(parsed)
            return self._last_ms
        if fallback is not None:
            return _epoch_ms(fallback)
        if self._last_ms is not None:
            return self._last_ms
        return int(time.time() * 1000)

    def _on_delivery(self, error: object, message: object) -> None:
        if error is not None:
            self.errors += 1

    def produce(self, emission: Emission, fallback: datetime | None = None) -> None:
        event = emission.event
        topic = topic_for(event.event_type)
        value = self._serializer(emission.schema_version)(
            event.to_dict(emission.schema_version), SerializationContext(topic, MessageField.VALUE)
        )
        key = None if event.event_id is None else event.event_id.encode("utf-8")
        timestamp = self._timestamp_ms(event.event_ts, fallback)
        while True:
            try:
                self._producer.produce(
                    topic,
                    key=key,
                    value=value,
                    timestamp=timestamp,
                    on_delivery=self._on_delivery,
                )
                break
            except BufferError:
                self._producer.poll(0.5)
        self._producer.poll(0)

    def flush(self) -> int:
        return self._producer.flush(FLUSH_TIMEOUT_S)
