import hashlib
import re
from datetime import datetime, timedelta
from pathlib import Path

from replayer.timeline import Change, build_timeline, shift_history


def _orders(changes: list[Change], order_id: str) -> list[Change]:
    return [c for c in changes if c.table == "orders" and dict(c.key)["order_id"] == order_id]


def _status(change: Change) -> object:
    return dict(change.values).get("order_status")


def test_created_before_items_before_approved(tmp_data_dir: Path) -> None:
    changes = build_timeline(tmp_data_dir, None, None)
    statuses = [_status(c) for c in _orders(changes, "o1")]
    assert statuses == ["created", "approved", "shipped", "delivered"]
    created = _orders(changes, "o1")[0]
    items = [c for c in changes if c.table == "order_items" and dict(c.key)["order_id"] == "o1"]
    approved = _orders(changes, "o1")[1]
    assert items
    assert all(created.seq < i.seq and i.ts <= approved.ts for i in items)
    assert all(i.seq < approved.seq for i in items)


def test_null_approved_at_skips_step(tmp_data_dir: Path) -> None:
    o3 = _orders(build_timeline(tmp_data_dir, None, None), "o3")
    assert [_status(c) for c in o3] == ["created", "shipped", "delivered"]
    assert [c.ts for c in o3] == sorted(c.ts for c in o3)


def test_final_status_applied_when_not_reached(tmp_data_dir: Path) -> None:
    o4 = _orders(build_timeline(tmp_data_dir, None, None), "o4")
    assert [_status(c) for c in o4] == ["created", "approved", "canceled"]
    assert o4[-1].ts == datetime(2017, 4, 1, 8, 30)


def test_review_answer_is_separate_update(tmp_data_dir: Path) -> None:
    reviews = [
        c
        for c in build_timeline(tmp_data_dir, None, None)
        if c.table == "order_reviews" and dict(c.key)["review_id"] == "r3"
    ]
    assert len(reviews) == 2
    assert dict(reviews[0].values)["review_answer_timestamp"] is None
    assert dict(reviews[1].values) == {"review_answer_timestamp": datetime(2017, 3, 7, 10)}
    assert reviews[0].ts == datetime(2017, 3, 6)


def test_window_filters_on_purchase(tmp_data_dir: Path) -> None:
    changes = build_timeline(tmp_data_dir, None, datetime(2017, 1, 2))
    order_ids = {dict(c.key)["order_id"] for c in changes if c.table == "orders"}
    assert order_ids == {"o1"}
    assert max(c.ts for c in changes) > datetime(2017, 1, 2)
    start_only = build_timeline(tmp_data_dir, datetime(2017, 3, 1), None)
    assert {dict(c.key)["order_id"] for c in start_only if c.table == "orders"} == {
        "o3",
        "o4",
        "o5",
    }


def test_timeline_is_sorted_and_deterministic(tmp_data_dir: Path) -> None:
    changes = build_timeline(tmp_data_dir, None, None)
    assert sorted(changes) == changes
    assert build_timeline(tmp_data_dir, None, None) == changes


def test_created_change_nulls_later_columns(tmp_data_dir: Path) -> None:
    created = _orders(build_timeline(tmp_data_dir, None, None), "o1")[0]
    values = dict(created.values)
    assert _status(created) == "created"
    for column in (
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
    ):
        assert column in values
        assert values[column] is None


def test_out_of_order_timestamps_are_clamped_but_values_keep_original(tmp_data_dir: Path) -> None:
    o5 = _orders(build_timeline(tmp_data_dir, None, None), "o5")
    assert [_status(c) for c in o5] == ["created", "approved", "shipped", "delivered"]
    assert [c.ts for c in o5] == sorted(c.ts for c in o5)
    shipped = o5[2]
    assert shipped.ts == datetime(2017, 5, 1, 12)
    assert dict(shipped.values)["order_delivered_carrier_date"] == datetime(2017, 5, 1, 10)


def test_payments_without_approval_are_emitted_at_purchase(tmp_data_dir: Path) -> None:
    payments = [
        c
        for c in build_timeline(tmp_data_dir, None, None)
        if c.table == "order_payments" and dict(c.key)["order_id"] == "o3"
    ]
    assert [c.ts for c in payments] == [datetime(2017, 3, 1, 9)]


def test_shift_history_rewrites_ids_and_times(tmp_data_dir: Path) -> None:
    changes = build_timeline(tmp_data_dir, None, None)
    span = timedelta(days=100)
    shifted = shift_history(changes, span, 2)
    assert len(shifted) == len(changes)
    for old, new in zip(changes, shifted, strict=True):
        assert new.ts == old.ts + span * 2
        assert new.seq == old.seq

    def expected(old: str) -> str:
        return hashlib.sha1(f"{old}:2".encode()).hexdigest()[:32]

    order_ids = {dict(c.key)["order_id"] for c in shifted if c.table == "orders"}
    assert order_ids == {expected(o) for o in ("o1", "o2", "o3", "o4", "o5")}
    assert all(re.fullmatch(r"[0-9a-f]{32}", str(i)) for i in order_ids)
    customer = next(c for c in shifted if c.table == "customers")
    assert dict(customer.key)["customer_id"] in {expected(f"c{n}") for n in range(1, 6)}
    assert dict(customer.values)["customer_unique_id"] in {expected(f"u{n}") for n in range(1, 6)}
    review = next(c for c in shifted if c.table == "order_reviews")
    assert dict(review.key)["review_id"] in {expected(r) for r in ("r1", "r2", "r3")}
    created = next(c for c in shifted if c.table == "orders" and _status(c) == "created")
    assert dict(created.values)["order_purchase_timestamp"] == created.ts
    assert dict(created.values)["customer_id"] in {expected(f"c{n}") for n in range(1, 6)}
