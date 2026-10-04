from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import numpy as np
import pandas as pd
import pytest
from prometheus_client import CollectorRegistry

from retail_ml.features import SELLER_FEATURES
from retail_ml.late_delivery.train import MODEL_INPUT_COLUMNS
from retail_ml.serving.model import LoadedModel
from retail_ml.streaming.score import (
    Scorer,
    dataset_utc,
    is_approval,
    kafka_record,
    order_rows,
    run,
    upsert_order_risk,
)

T0 = 1_700_000_000_000  # source.ts_ms (Postgres commit, wall clock)
WALL = datetime(2018, 7, 1, 10, 30, tzinfo=UTC)  # Debezium timestamp-millis = São Paulo wall clock


def after(order_id: str = "o1", status: str = "approved") -> dict[str, Any]:
    return {
        "order_id": order_id,
        "customer_id": "c1",
        "order_status": status,
        "order_purchase_timestamp": datetime(2018, 7, 1, 9, 0, tzinfo=UTC),
        "order_approved_at": WALL,
        "order_delivered_carrier_date": None,
        "order_delivered_customer_date": None,
        "order_estimated_delivery_date": datetime(2018, 7, 20, tzinfo=UTC),
    }


def envelope(
    op: str = "u", before: str | None = "created", status: str = "approved", order_id: str = "o1"
) -> dict[str, Any]:
    return {
        "op": op,
        "before": None if before is None else after(order_id, before),
        "after": None if op == "d" else after(order_id, status),
        "source": {"ts_ms": T0, "table": "orders"},
        "ts_ms": T0 + 5,
    }


@pytest.mark.parametrize(
    ("env", "keep"),
    [
        (envelope("u", "created", "approved"), True),
        (envelope("c", None, "approved"), True),
        (envelope("u", "approved", "approved"), False),  # e.g. a later column update
        (envelope("u", "approved", "shipped"), False),
        (envelope("u", "created", "canceled"), False),
        (envelope("r", None, "approved"), False),  # snapshot read is history, not a transition
        (envelope("d", "approved"), False),
    ],
)
def test_only_transitions_to_approved_are_kept(env: dict[str, Any], keep: bool) -> None:
    assert is_approval(env) is keep


def test_debezium_wall_clock_is_converted_from_sao_paulo_to_utc() -> None:
    assert dataset_utc(WALL) == pd.Timestamp("2018-07-01 13:30", tz="UTC")  # UTC-3
    assert dataset_utc(datetime(2018, 7, 1, 10, 30)) == pd.Timestamp("2018-07-01 13:30", tz="UTC")
    assert dataset_utc(1530441000000) == pd.Timestamp("2018-07-01 13:30", tz="UTC")  # raw millis
    assert dataset_utc(None) is None


DETAILS = {
    "customer_unique_id": "u1",
    "seller_id": "s1",
    "n_items": 2,
    "n_sellers": 1,
    "total_price": 100.0,
    "total_freight": 20.0,
    "product_category": "toys",
    "payment_type": None,  # payments land after the approval event in the replay
    "payment_installments": None,
    "customer_state": "SP",
    "customer_lat": -23.5,
    "customer_lng": -46.6,
    "seller_state": "RJ",
    "seller_lat": -22.9,
    "seller_lng": -43.2,
}


def test_order_rows_build_the_training_contract_from_after_image_and_lookup() -> None:
    df = order_rows([after("o1"), after("o2")], {"o1": DETAILS})
    assert df.loc[0, "order_approved_ts_utc"] == pd.Timestamp("2018-07-01 13:30", tz="UTC")
    assert df.loc[0, "order_purchase_ts_utc"] == pd.Timestamp("2018-07-01 12:00", tz="UTC")
    estimated = pd.Timestamp("2018-07-20 03:00", tz="UTC")
    assert df.loc[0, "order_estimated_delivery_ts_utc"] == estimated
    assert df.loc[0, "seller_id"] == "s1" and df.loc[0, "customer_id"] == "c1"
    assert df.loc[1, "n_items"] == 0 and df.loc[1, "seller_id"] is None  # no rows yet
    missing = set(MODEL_INPUT_COLUMNS) - set(SELLER_FEATURES) - set(df.columns)
    assert not missing


@pytest.fixture
def conn() -> Iterator[sqlite3.Connection]:
    c = sqlite3.connect(":memory:", isolation_level=None)
    c.execute("ATTACH DATABASE ':memory:' AS ml")
    c.execute(
        "CREATE TABLE ml.order_risk (order_id text primary key, probability double precision,"
        " model_version text, scored_at timestamptz, source_ts timestamptz,"
        " latency_ms double precision)"
    )
    yield c
    c.close()


def risk(order_id: str, probability: float, scored: str) -> dict[str, Any]:
    return {
        "order_id": order_id,
        "probability": probability,
        "model_version": "3",
        "scored_ts": scored,
        "source_ts": "2023-11-14T22:13:20+00:00",
        "latency_ms": 120.0,
    }


def test_upsert_is_idempotent_and_reports_first_time_scores(conn: sqlite3.Connection) -> None:
    first = [risk("o1", 0.2, "t1"), risk("o2", 0.4, "t1")]
    assert upsert_order_risk(conn, first, placeholder="?") == {"o1", "o2"}
    again = [risk("o1", 0.9, "t2"), risk("o3", 0.5, "t2")]
    assert upsert_order_risk(conn, again, placeholder="?") == {"o3"}
    rows = conn.execute("SELECT order_id, probability, scored_at FROM ml.order_risk ORDER BY 1")
    assert rows.fetchall() == [("o1", 0.2, "t1"), ("o2", 0.4, "t1"), ("o3", 0.5, "t2")]
    assert upsert_order_risk(conn, [], placeholder="?") == set()


class StubModel:
    def __init__(self) -> None:
        self.inputs: list[pd.DataFrame] = []

    def predict(self, df: pd.DataFrame) -> Any:
        self.inputs.append(df)
        return np.full(len(df), 0.25)


class FakeWrites:
    def __init__(self) -> None:
        self.risk: dict[str, dict[str, Any]] = {}
        self.calls: list[str] = []
        self.produced: list[tuple[str, bytes]] = []
        self.appended: list[pd.DataFrame] = []

    def upsert(self, rows: list[dict[str, Any]]) -> set[str]:
        self.calls.append("upsert")
        new = {r["order_id"] for r in rows} - set(self.risk)
        self.risk.update({r["order_id"]: r for r in rows if r["order_id"] in new})
        return new

    def produce(self, rows: list[dict[str, Any]]) -> None:
        self.calls.append("produce")
        self.produced += [(r["order_id"], kafka_record(r)) for r in rows]

    def append(self, frame: pd.DataFrame) -> None:
        self.calls.append("append")
        self.appended.append(frame)

    def flush(self) -> None:
        self.calls.append("flush")


class FakeMessage:
    def __init__(self, value: Any) -> None:
        self._value = value

    def value(self) -> Any:
        return self._value

    def error(self) -> None:
        return None


class FakeConsumer:
    """Each batch = what one poll + drain returns (poll: first message, consume: the rest)."""

    def __init__(self, batches: list[list[FakeMessage]], writes: FakeWrites) -> None:
        self.batches, self.writes = batches, writes
        self.rest: list[FakeMessage] = []
        self.commits = 0

    def poll(self, timeout: float) -> FakeMessage | None:
        batch = self.batches.pop(0) if self.batches else []
        self.rest = batch[1:]
        return batch[0] if batch else None

    def consume(self, num_messages: int, timeout: float) -> list[FakeMessage]:
        assert timeout == 0  # drain only: never wait for a batch to fill
        rest, self.rest = self.rest, []
        return rest

    def commit(self, asynchronous: bool = True) -> None:
        self.writes.calls.append("commit")
        self.commits += 1


def _scorer(writes: FakeWrites, model: StubModel, registry: CollectorRegistry) -> Scorer:
    sellers = lambda ids: pd.DataFrame(  # noqa: E731
        {c: [np.nan] * len(ids) for c in SELLER_FEATURES}
    )
    return Scorer(
        model=LoadedModel("3", model),
        lookup=lambda ids: {i: DETAILS for i in ids},
        sellers=sellers,
        writes=writes,
        registry=registry,
        now=lambda: datetime.fromtimestamp((T0 + 800) / 1000, UTC),
    )


def _latency_count(registry: CollectorRegistry) -> float:
    return registry.get_sample_value("stream_score_latency_seconds_count") or 0.0


def test_scorer_writes_contract_rows_and_counts_latency_once() -> None:
    writes, model, registry = FakeWrites(), StubModel(), CollectorRegistry()
    scorer = _scorer(writes, model, registry)
    before = _latency_count(registry)
    rows = scorer.process([envelope(order_id="o1"), envelope("u", "approved", "shipped", "o2")])
    assert [r["order_id"] for r in rows] == ["o1"]
    (X,) = model.inputs
    assert list(X.columns) == MODEL_INPUT_COLUMNS
    assert X.loc[0, "order_approved_ts_utc"] == pd.Timestamp("2018-07-01 13:30")
    record = json.loads(writes.produced[0][1])
    assert record == {
        "order_id": "o1",
        "probability": 0.25,
        "model_version": "3",
        "approved_ts": "2018-07-01T13:30:00+00:00",
        "scored_ts": "2023-11-14T22:13:20.800000+00:00",
        "source_ts_ms": T0,
    }
    assert writes.risk["o1"]["latency_ms"] == pytest.approx(800.0)
    assert _latency_count(registry) - before == 1
    assert registry.get_sample_value("stream_score_latency_seconds_bucket", {"le": "1.0"})
    scorer.process([envelope(order_id="o1")])  # redelivery after a restart
    assert _latency_count(registry) - before == 1
    assert len(writes.risk) == 1


def test_run_commits_offsets_only_after_the_batch_is_written() -> None:
    writes, registry = FakeWrites(), CollectorRegistry()
    batches = [
        [
            FakeMessage(envelope(order_id="o1")),
            FakeMessage(None),  # tombstone
            FakeMessage(envelope(order_id="o2")),
        ],
        [FakeMessage(envelope("u", "approved", "shipped", "o1"))],
        [],
    ]
    consumer = FakeConsumer(batches, writes)
    ticks = iter([0.0, 0.1, 9.0])  # one tick per poll
    loops = iter(range(3))
    run(
        consumer,
        lambda m: m.value(),
        _scorer(writes, StubModel(), registry),
        batch_rows=2,
        batch_seconds=5.0,
        clock=lambda: next(ticks),
        running=lambda: next(loops, None) is not None,
    )
    # batch 1: 2 scores reach batch_rows -> Delta append, producer flush, then commit
    assert writes.calls[:5] == ["upsert", "produce", "append", "flush", "commit"]
    assert len(writes.appended[0]) == 2
    # batch 2: nothing to score; its offset is committed batch_seconds after it arrived
    assert writes.calls[5:] == ["commit"]
    assert consumer.commits == 2
