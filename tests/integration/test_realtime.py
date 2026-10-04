"""Realtime end-to-end tests with measured latencies (printed; run with -rP to see them).

- Session path: a simulated session (clickstream-sim generator + Avro producer) -> bronze events
  Delta -> spark-realtime -> Feast online `session_features` with all its events counted, then
  `/recommend` must rerank. A warm-up session first lets spark-realtime find the tables.
- CDC path: an approved-order transition written with `replayer apply_change` -> Debezium ->
  stream-score -> `ml.order_risk`.

Mutates the live stack: it writes an `e2e…` order (with its items, deleted afterwards; CDC carries
the deletes too) and two sessions' events (bronze is append-only, they stay), stamped just after the
latest order in Postgres so they never move the dataset-time watermarks backwards. Requires
ALLOW_RESEED=1, `make up PROFILE=ingest` + `make up PROFILE=realtime`, late-delivery and
recommender champions, published candidates and an applied Feast registry.
"""

import json
import os
import random
import time
import uuid
from collections.abc import Callable, Iterator
from datetime import datetime, timedelta
from itertools import count
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

import psycopg
import pytest
from clickstream_sim.cli import load_catalogue
from clickstream_sim.events import Event, OrderRef, converting_session
from clickstream_sim.faults import Emission
from clickstream_sim.producer import EventProducer
from dotenv import find_dotenv, load_dotenv
from replayer.live import apply_change
from replayer.timeline import Change

load_dotenv(find_dotenv(usecwd=True))

pytestmark = pytest.mark.realtime

ROOT = Path(__file__).resolve().parents[2]
FEAST_URL = os.environ.get("FEAST_SERVER_URL") or "http://127.0.0.1:6566"
RECOMMEND_URL = os.environ.get("RECOMMEND_URL") or (
    f"http://127.0.0.1:{os.environ.get('SERVING_PORT') or 8000}"
)
TARGET_S = 20.0
# bronze writes each event type's Delta table in turn (~20 s per micro-batch), then spark-realtime
SESSION_TARGET_S = 45.0
# polled for longer than the target so a miss still reports its latency
MEASURE_S = 60.0
# the first session also waits for spark-realtime to find the bronze tables and start its queries
WARMUP_S = 300.0
POLL_S = 0.5
SESSION_TYPES = {"add_to_cart", "checkout_started", "page_view", "product_view", "search"}


def _reachable(url: str) -> bool:
    try:
        with urlopen(url, timeout=3):  # noqa: S310
            return True
    except OSError:
        return False


# a fixture, not a module-level skip: `-m ingest` deselects these tests without reporting a skip
@pytest.fixture(scope="module", autouse=True)
def _realtime_stack() -> None:
    if os.environ.get("ALLOW_RESEED") != "1":
        pytest.skip("set ALLOW_RESEED=1 to write test rows to the stack")
    if not (_reachable(f"{FEAST_URL}/health") and _reachable(f"{RECOMMEND_URL}/health")):
        pytest.skip("feast-server / serving not reachable (PROFILE=realtime)")


def _post(url: str, body: dict[str, Any]) -> Any:
    req = Request(url, json.dumps(body).encode(), {"Content-Type": "application/json"})
    with urlopen(req, timeout=10) as resp:  # noqa: S310
        return json.load(resp)


def _poll[T](what: str, timeout_s: float, probe: Callable[[], T | None]) -> tuple[T, float]:
    started = time.monotonic()
    while (found := probe()) is None:
        if time.monotonic() - started > timeout_s:
            pytest.fail(f"timed out after {timeout_s:.0f} s waiting for {what}")
        time.sleep(POLL_S)
    return found, time.monotonic() - started


def _anchor(conn: psycopg.Connection) -> tuple[datetime, str, list[tuple[Any, ...]]]:
    """Latest dataset time + 2 h, and a delivered order (customer, items) to copy."""
    row = conn.execute(
        "select o.order_id, o.customer_id, (select max(order_purchase_timestamp) from olist.orders)"
        " from olist.orders o where o.order_status = 'delivered'"
        " and exists (select 1 from olist.order_items i where i.order_id = o.order_id)"
        " order by o.order_id limit 1"
    ).fetchone()
    assert row is not None, "no delivered order with items to copy"
    template, customer, latest = row
    items = conn.execute(
        "select order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value"
        " from olist.order_items where order_id = %s order by order_item_id",
        (template,),
    ).fetchall()
    return latest.replace(microsecond=0) + timedelta(hours=2), customer, items


def _session(order: OrderRef, dsn: str) -> list[Event]:
    catalogue = load_catalogue(dsn)
    # the first seed whose session has every type spark-realtime waits for (search is random)
    for seed in count():
        events = converting_session(order, catalogue, random.Random(seed))
        if {e.event_type for e in events} >= SESSION_TYPES:
            return events
    raise AssertionError("unreachable")


def _online_events(session_id: str) -> int | None:
    try:
        body = _post(
            f"{FEAST_URL}/get-online-features",
            {"features": ["session_features:n_events"], "entities": {"session_id": [session_id]}},
        )
    except OSError:  # feast-server briefly unreachable (restart / load); keep polling
        return None
    names = body["metadata"]["feature_names"]
    value = body["results"][names.index("n_events")]["values"][0]
    return None if value is None else int(value)


def _emit_session(order: OrderRef, dsn: str, timeout_s: float) -> tuple[str, float, float]:
    """Produce the session; seconds until its first Feast row and until all its events count."""
    events = _session(order, dsn)
    producer = EventProducer(
        os.environ.get("KAFKA_BOOTSTRAP") or "127.0.0.1:19092",
        os.environ.get("SCHEMA_REGISTRY_URL") or "http://127.0.0.1:18081",
        ROOT / "ingestion" / "schemas" / "events",
    )
    for event in events:
        producer.produce(Emission(event, 0.0, False, 1), order.purchase_ts)
    assert producer.flush() == 0 and producer.errors == 0
    session_id = events[0].session_id
    assert session_id is not None

    started = time.monotonic()
    first: float | None = None
    while True:
        seen = _online_events(session_id)
        elapsed = time.monotonic() - started
        if seen is not None and first is None:
            first = elapsed
        if seen == len(events) and first is not None:
            return session_id, first, elapsed
        if elapsed > timeout_s:
            pytest.fail(
                f"session {session_id}: Feast online n_events={seen} of {len(events)} after"
                f" {timeout_s:.0f} s (first row after {first} s)"
            )
        time.sleep(POLL_S)


@pytest.fixture(scope="module")
def conn() -> Iterator[psycopg.Connection]:
    with psycopg.connect(os.environ["POSTGRES_DSN"], autocommit=True) as c:
        yield c


@pytest.fixture(scope="module")
def stamp(conn: psycopg.Connection) -> tuple[datetime, str, list[tuple[Any, ...]]]:
    return _anchor(conn)


def _order(customer: str, ts: datetime, items: list[tuple[Any, ...]]) -> OrderRef:
    return OrderRef(f"e2e{uuid.uuid4().hex[:29]}", customer, ts, tuple(str(i[1]) for i in items))


def _report(values: dict[str, float]) -> None:
    print("realtime latencies:", json.dumps({k: round(v, 3) for k, v in values.items()}))


def test_session_reaches_feast_and_reranks(
    stamp: tuple[datetime, str, list[tuple[Any, ...]]],
) -> None:
    dsn = os.environ["POSTGRES_DSN"]
    anchor, customer, items = stamp
    report: dict[str, float] = {}
    _, _, report["warmup_session_to_feast_s"] = _emit_session(
        _order(customer, anchor, items), dsn, WARMUP_S
    )
    session_id, report["session_first_row_s"], report["session_to_feast_s"] = _emit_session(
        _order(customer, anchor + timedelta(minutes=1), items), dsn, MEASURE_S
    )
    started = time.monotonic()
    rec = _post(f"{RECOMMEND_URL}/recommend", {"session_id": session_id, "k": 10})
    report["recommend_s"] = time.monotonic() - started
    _report(report)
    assert rec["strategy"] == "rerank", rec
    assert rec["model_version"] and len(rec["items"]) == 10, rec
    assert report["session_to_feast_s"] <= SESSION_TARGET_S


def test_approval_reaches_order_risk(
    conn: psycopg.Connection, stamp: tuple[datetime, str, list[tuple[Any, ...]]]
) -> None:
    anchor, customer, items = stamp
    risk = _order(customer, anchor + timedelta(minutes=2), items)
    purchase = risk.purchase_ts
    report: dict[str, float] = {}
    try:
        apply_change(
            conn,
            Change(
                purchase,
                0,
                "orders",
                (("order_id", risk.order_id),),
                (
                    ("customer_id", customer),
                    ("order_status", "created"),
                    ("order_purchase_timestamp", purchase),
                    ("order_estimated_delivery_date", purchase + timedelta(days=20)),
                ),
            ),
        )
        for item_id, product, seller, limit, price, freight in items:
            apply_change(
                conn,
                Change(
                    purchase,
                    0,
                    "order_items",
                    (("order_id", risk.order_id), ("order_item_id", item_id)),
                    (
                        ("product_id", product),
                        ("seller_id", seller),
                        ("shipping_limit_date", limit),
                        ("price", price),
                        ("freight_value", freight),
                    ),
                ),
            )
        approved = purchase + timedelta(minutes=10)
        apply_change(
            conn,
            Change(
                approved,
                0,
                "orders",
                (("order_id", risk.order_id),),
                (("order_status", "approved"), ("order_approved_at", approved)),
            ),
        )

        def scored() -> tuple[Any, ...] | None:
            return conn.execute(
                "select latency_ms, source_ts, scored_at, probability from ml.order_risk"
                " where order_id = %s",
                (risk.order_id,),
            ).fetchone()

        (latency_ms, source_ts, scored_at, probability), waited = _poll(
            f"ml.order_risk row for {risk.order_id}", MEASURE_S, scored
        )
    finally:
        conn.execute("delete from ml.order_risk where order_id = %s", (risk.order_id,))
        conn.execute("delete from olist.order_items where order_id = %s", (risk.order_id,))
        conn.execute("delete from olist.orders where order_id = %s", (risk.order_id,))
    report["approve_to_order_risk_s"] = waited
    report["order_risk_latency_ms"] = float(latency_ms)
    _report(report)
    assert 0.0 <= probability <= 1.0
    # source_ts = Debezium source.ts_ms (Postgres commit), scored_at = stream-score's clock
    assert scored_at >= source_ts
    assert waited <= TARGET_S
