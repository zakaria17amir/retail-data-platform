from collections import OrderedDict
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from clickstream_sim.events import OrderRef

ORDERS_TOPIC = "cdc.olist.orders"
ITEMS_TOPIC = "cdc.olist.order_items"
MAX_SEEN = 50_000
CREATE_OPS = ("c", "r")
ORDER_OPS = ("c", "r", "u")


def _naive_utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC).replace(tzinfo=None) if value.tzinfo else value
    return datetime.fromtimestamp(int(value) / 1000, tz=UTC).replace(tzinfo=None)


class OrderFeed:
    def __init__(
        self,
        bootstrap: str = "",
        registry_url: str = "",
        *,
        consumer: Any = None,
        deserializer: Callable[[Any], dict[str, Any] | None] | None = None,
        max_seen: int = MAX_SEEN,
        group_id: str = "clickstream-sim",
    ) -> None:
        if consumer is None:
            consumer = Consumer(
                {
                    "bootstrap.servers": bootstrap,
                    "group.id": group_id,
                    "auto.offset.reset": "latest",
                    "enable.auto.commit": True,
                }
            )
            consumer.subscribe([ORDERS_TOPIC, ITEMS_TOPIC])
        if deserializer is None:
            avro = AvroDeserializer(SchemaRegistryClient.new_client({"url": registry_url}))

            def deserializer(message: Any) -> dict[str, Any] | None:
                value = message.value()
                if value is None:
                    return None
                decoded = avro(value, SerializationContext(message.topic(), MessageField.VALUE))
                return decoded if isinstance(decoded, dict) else None

        self._consumer = consumer
        self._deserialize = deserializer
        self._max_seen = max_seen
        self._pending: OrderedDict[str, tuple[str | None, datetime]] = OrderedDict()
        self._seen: OrderedDict[str, None] = OrderedDict()

    def poll(self, timeout_s: float) -> OrderRef | None:
        message = self._consumer.poll(timeout_s)
        if message is None or message.error():
            return None
        envelope = self._deserialize(message)
        if envelope is None or not envelope.get("after"):
            return None
        after = envelope["after"]
        op = envelope.get("op")
        if message.topic() == ORDERS_TOPIC:
            self._remember_order(op, after)
            return None
        if message.topic() == ITEMS_TOPIC:
            return self._order_ref(op, after, message)
        return None

    def _remember_order(self, op: str | None, after: dict[str, Any]) -> None:
        order_id = after.get("order_id")
        stamp = after.get("order_purchase_timestamp")
        if op not in ORDER_OPS or order_id is None or stamp is None or order_id in self._pending:
            return
        self._pending[order_id] = (after.get("customer_id"), _naive_utc(stamp))
        while len(self._pending) > self._max_seen:
            self._pending.popitem(last=False)

    def _order_ref(self, op: str | None, after: dict[str, Any], message: Any) -> OrderRef | None:
        order_id = after.get("order_id")
        product_id = after.get("product_id")
        if order_id is None or product_id is None or order_id in self._seen:
            return None
        if op not in CREATE_OPS and not (op == "u" and order_id in self._pending):
            return None
        self._seen[order_id] = None
        while len(self._seen) > self._max_seen:
            self._seen.popitem(last=False)
        if order_id in self._pending:
            customer_id, purchase_ts = self._pending.pop(order_id)
        else:
            customer_id, purchase_ts = None, _naive_utc(message.timestamp()[1])
        return OrderRef(order_id, customer_id, purchase_ts, (product_id,))
