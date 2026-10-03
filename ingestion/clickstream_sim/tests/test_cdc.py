from datetime import UTC, datetime
from typing import Any

from clickstream_sim.cdc import OrderFeed

PURCHASE = datetime(2017, 10, 2, 10, 0, 0)
PURCHASE_MS = 1506938400000


class FakeMessage:
    def __init__(self, topic: str, value: dict[str, Any] | None, timestamp_ms: int = 0) -> None:
        self._topic = topic
        self.payload = value
        self._timestamp = timestamp_ms

    def topic(self) -> str:
        return self._topic

    def value(self) -> dict[str, Any] | None:
        return self.payload

    def error(self) -> None:
        return None

    def timestamp(self) -> tuple[int, int]:
        return (1, self._timestamp)


class FakeConsumer:
    def __init__(self, messages: list[FakeMessage]) -> None:
        self._messages = list(messages)

    def poll(self, timeout: float) -> FakeMessage | None:
        return self._messages.pop(0) if self._messages else None


def _order(op: str, order_id: str = "o1", purchase: Any = PURCHASE_MS) -> FakeMessage:
    after = {"order_id": order_id, "customer_id": "c1", "order_purchase_timestamp": purchase}
    return FakeMessage("cdc.olist.orders", {"op": op, "before": None, "after": after})


def _item(op: str, order_id: str = "o1", product: str = "p1", ts: int = 0) -> FakeMessage:
    after = {"order_id": order_id, "product_id": product}
    return FakeMessage("cdc.olist.order_items", {"op": op, "before": None, "after": after}, ts)


def _drain(feed: OrderFeed, count: int) -> list[Any]:
    return [feed.poll(0) for _ in range(count)]


def _feed(messages: list[FakeMessage], **kwargs: Any) -> OrderFeed:
    return OrderFeed(consumer=FakeConsumer(messages), deserializer=lambda m: m.value(), **kwargs)


def test_first_item_of_an_order_emits_with_order_context() -> None:
    results = _drain(_feed([_order("c"), _item("c", product="p9")]), 2)
    assert results[0] is None
    ref = results[1]
    assert (ref.order_id, ref.customer_id, ref.purchase_ts, ref.product_ids) == (
        "o1",
        "c1",
        PURCHASE,
        ("p9",),
    )


def test_later_items_of_the_same_order_are_ignored() -> None:
    results = _drain(_feed([_order("c"), _item("c"), _item("c", product="p2")]), 3)
    assert results[1] is not None
    assert results[2] is None


def test_datetime_purchase_timestamp_is_accepted() -> None:
    aware = datetime(2017, 10, 2, 10, 0, 0, tzinfo=UTC)
    ref = _drain(_feed([_order("c", purchase=aware), _item("c")]), 2)[1]
    assert ref.purchase_ts == PURCHASE
    assert ref.purchase_ts.tzinfo is None


def test_item_without_seen_order_has_no_customer_and_uses_message_timestamp() -> None:
    ref = _drain(_feed([_item("r", ts=PURCHASE_MS)]), 1)[0]
    assert ref.customer_id is None
    assert ref.purchase_ts == PURCHASE


def test_update_item_only_triggers_for_an_order_seen_in_this_run() -> None:
    results = _drain(_feed([_item("u", order_id="old"), _order("u"), _item("u")]), 3)
    assert results[0] is None
    assert results[2] is not None


def test_other_ops_and_tombstones_are_ignored() -> None:
    messages = [_item("d"), FakeMessage("cdc.olist.order_items", None), _order("d")]
    assert _drain(_feed(messages), 3) == [None, None, None]


def test_seen_set_is_bounded() -> None:
    messages = [_item("c", order_id=f"o{i}") for i in range(4)] + [_item("c", order_id="o0")]
    results = _drain(_feed(messages, max_seen=2), 5)
    assert all(r is not None for r in results[:4])
    assert results[4] is not None
