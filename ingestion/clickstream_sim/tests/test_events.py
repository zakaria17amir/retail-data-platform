import json
import random
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from clickstream_sim.events import Catalogue, Event, OrderRef, browsing_session, converting_session

SCHEMAS = Path(__file__).resolve().parents[2] / "schemas" / "events"


@pytest.fixture
def catalogue() -> Catalogue:
    by_category = {
        "toys": ("p1", "p2", "p3", "p4"),
        "books": ("p5", "p6", "p7"),
    }
    category_of = {p: c for c, ps in by_category.items() for p in ps}
    return Catalogue(by_category=by_category, category_of=category_of)


@pytest.fixture
def order() -> OrderRef:
    return OrderRef(
        order_id="o1",
        customer_id="c1",
        purchase_ts=datetime(2017, 6, 1, 12, 0, 0),
        product_ids=("p2", "p6"),
    )


@pytest.fixture
def rng() -> random.Random:
    return random.Random(7)


def _ts(event: Event) -> datetime:
    return datetime.fromisoformat(event.event_ts)


def test_converting_session_ends_in_checkout_with_order_id(
    order: OrderRef, catalogue: Catalogue, rng: random.Random
) -> None:
    events = converting_session(order, catalogue, rng)
    assert events[0].event_type == "page_view"
    assert events[-1].event_type == "checkout_started"
    assert events[-1].order_id == order.order_id
    carts = [e for e in events if e.event_type == "add_to_cart"]
    assert sorted(e.product_id or "" for e in carts) == sorted(order.product_ids)
    assert all(e.quantity == 1 for e in carts)
    assert len({e.session_id for e in events}) == 1
    assert {e.customer_id for e in events} == {order.customer_id}
    assert len({e.event_id for e in events}) == len(events)


def test_converting_session_ts_strictly_increasing_within_30min(
    order: OrderRef, catalogue: Catalogue, rng: random.Random
) -> None:
    stamps = [_ts(e) for e in converting_session(order, catalogue, rng)]
    assert stamps == sorted(set(stamps))
    assert stamps[0] >= order.purchase_ts - timedelta(minutes=30)
    assert stamps[-1] <= order.purchase_ts


def test_product_views_share_category_and_end_on_purchased_product(
    order: OrderRef, catalogue: Catalogue
) -> None:
    for seed in range(20):
        events = converting_session(order, catalogue, random.Random(seed))
        views = [e.product_id for e in events if e.event_type == "product_view"]
        assert 2 <= len(views) <= 5
        assert views[-1] in order.product_ids
        assert {catalogue.category_of[p or ""] for p in views} == {
            catalogue.category_of[views[-1] or ""]
        }


def test_browsing_session_has_no_checkout_and_no_customer(catalogue: Catalogue) -> None:
    at = datetime(2017, 6, 1, 9, 0, 0)
    for seed in range(20):
        events = browsing_session(at, catalogue, random.Random(seed))
        assert events[0].event_type == "page_view"
        assert all(e.event_type != "checkout_started" for e in events)
        assert all(e.customer_id is None and e.order_id is None for e in events)
        assert any(e.event_type == "product_view" for e in events)
        stamps = [_ts(e) for e in events]
        assert stamps == sorted(set(stamps))
        assert stamps[0] == at


def test_same_seed_same_events(order: OrderRef, catalogue: Catalogue) -> None:
    first = converting_session(order, catalogue, random.Random(3))
    assert first == converting_session(order, catalogue, random.Random(3))
    at = datetime(2017, 6, 1)
    assert browsing_session(at, catalogue, random.Random(3)) == browsing_session(
        at, catalogue, random.Random(3)
    )
    assert first != converting_session(order, catalogue, random.Random(4))


def test_to_dict_v1_has_no_utm_and_matches_schema_fields(
    order: OrderRef, catalogue: Catalogue, rng: random.Random
) -> None:
    event = converting_session(order, catalogue, rng)[0]
    v1_fields = [
        f["name"] for f in json.loads((SCHEMAS / "clickstream_event.v1.avsc").read_text())["fields"]
    ]
    v2_fields = [
        f["name"] for f in json.loads((SCHEMAS / "clickstream_event.v2.avsc").read_text())["fields"]
    ]
    assert list(event.to_dict(1)) == v1_fields
    assert "utm_campaign" not in event.to_dict(1)
    assert list(event.to_dict(2)) == v2_fields
    assert event.to_dict(2)["utm_campaign"] is None
