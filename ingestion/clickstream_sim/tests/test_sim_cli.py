import json
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from clickstream_sim import cli
from clickstream_sim.cli import SimConfig, report, run
from clickstream_sim.events import Catalogue, OrderRef
from clickstream_sim.faults import Emission
from clickstream_sim.feedback import Recommendations

CATALOGUE = Catalogue.from_rows([("p1", "toys"), ("p2", "toys"), ("p3", "books"), ("p4", None)])


class FakeFeed:
    def __init__(self, orders: list[OrderRef], clock: list[float]) -> None:
        self._orders = list(orders)
        self._clock = clock
        self.timeouts: list[float] = []

    def poll(self, timeout_s: float) -> OrderRef | None:
        self.timeouts.append(timeout_s)
        if self._orders:
            return self._orders.pop(0)
        self._clock[0] += timeout_s
        return None


class FakeProducer:
    def __init__(self, clock: list[float], errors: int = 0, unflushed: int = 0) -> None:
        self._clock = clock
        self.errors = errors
        self._unflushed = unflushed
        self.sent: list[tuple[float, Emission, datetime | None]] = []

    def produce(self, emission: Emission, fallback: datetime | None = None) -> None:
        self.sent.append((self._clock[0], emission, fallback))

    def flush(self) -> int:
        return self._unflushed


def _orders(n: int) -> list[OrderRef]:
    return [
        OrderRef(f"o{i}", f"c{i}", datetime(2017, 10, 2, 9 + i), ("p1", "p3")) for i in range(n)
    ]


def _cfg(**overrides: Any) -> SimConfig:
    values: dict[str, Any] = {
        "bootstrap": "unused",
        "registry_url": "unused",
        "postgres_dsn": "unused",
        "speed": 1e9,
        "late_rate": 0.2,
        "late_max_hours": 48,
        "dup_rate": 0.2,
        "bad_rate": 0.1,
        "schema_evolve_after": 0,
        "browsing_ratio": 3,
        "seed": 42,
    }
    values.update(overrides)
    return SimConfig(**values)


def _run_full(
    cfg: SimConfig,
    max_events: int | None,
    orders: int = 6,
    summary: Path | None = None,
    errors: int = 0,
    unflushed: int = 0,
) -> tuple[dict[str, int], FakeProducer, FakeFeed]:
    clock = [0.0]
    producer = FakeProducer(clock, errors, unflushed)
    feed = FakeFeed(_orders(orders), clock)

    def sleep(seconds: float) -> None:
        clock[0] += seconds

    summary_counts = run(
        cfg,
        max_events=max_events,
        summary_file=summary,
        sleep=sleep,
        now=lambda: clock[0],
        feed=feed,
        producer=producer,
        catalogue=CATALOGUE,
    )
    return summary_counts, producer, feed


def _run(
    cfg: SimConfig, max_events: int | None, orders: int = 6, summary: Path | None = None
) -> tuple[dict[str, int], FakeProducer]:
    summary_counts, producer, _ = _run_full(cfg, max_events, orders, summary)
    return summary_counts, producer


def test_delivery_errors_and_unflushed_are_reported(capsys: pytest.CaptureFixture[str]) -> None:
    summary, _, _ = _run_full(_cfg(), max_events=20, errors=2, unflushed=3)
    assert (summary["delivery_errors"], summary["unflushed"]) == (2, 3)
    assert report(summary) == 1
    assert "2 delivery errors, 3 messages unflushed" in capsys.readouterr().err


def test_clean_summary_reports_success(capsys: pytest.CaptureFixture[str]) -> None:
    summary, _, _ = _run_full(_cfg(), max_events=20)
    assert report(summary) == 0
    assert capsys.readouterr().err == ""


def test_feed_poll_timeout_is_bounded_while_emissions_are_pending() -> None:
    _, _, feed = _run_full(_cfg(speed=1e5, late_rate=1.0, dup_rate=0.0), max_events=60)
    assert max(feed.timeouts[1:]) <= 0.1


def test_summary_adds_up() -> None:
    summary, producer = _run(_cfg(), max_events=40)
    assert summary["events_unique"] == 40
    assert summary["produced"] == summary["events_unique"] + summary["duplicates"]
    assert len(producer.sent) == summary["produced"]
    assert summary["duplicates"] == sum(1 for _, e, _ in producer.sent if e.is_duplicate)
    assert summary["late"] >= 1
    assert summary["bad"] >= 1
    assert summary["schema_v2"] == 0


def test_null_id_duplicates_are_counted() -> None:
    summary, producer = _run(_cfg(bad_rate=1.0, dup_rate=1.0), max_events=60)
    expected = sum(1 for _, e, _ in producer.sent if e.is_duplicate and e.event.event_id is None)
    assert expected >= 1
    assert summary["null_id_duplicates"] == expected


def test_emissions_are_produced_in_due_order() -> None:
    _, producer = _run(_cfg(), max_events=60)
    times = [t for t, _, _ in producer.sent]
    assert times == sorted(times)
    assert times[-1] > times[0]


def test_every_emission_carries_dataset_time_fallback() -> None:
    _, producer = _run(_cfg(), max_events=20)
    assert all(fallback is not None for _, _, fallback in producer.sent)


def test_schema_flips_to_v2_after_threshold() -> None:
    summary, producer = _run(_cfg(schema_evolve_after=10, dup_rate=0, late_rate=0), max_events=30)
    versions = [e.schema_version for _, e, _ in producer.sent]
    assert versions[:10] == [1] * 10
    assert versions[10:] == [2] * 20
    assert summary["schema_v2"] == 20


def test_no_flip_when_threshold_is_zero() -> None:
    _, producer = _run(_cfg(schema_evolve_after=0), max_events=30)
    assert {e.schema_version for _, e, _ in producer.sent} == {1}


def test_same_seed_same_summary_and_summary_file(tmp_path: Path) -> None:
    target = tmp_path / "sim.json"
    first, _ = _run(_cfg(), max_events=50, summary=target)
    second, _ = _run(_cfg(), max_events=50)
    assert first == second
    assert json.loads(target.read_text()) == first


def _event_ids(orders: list[OrderRef]) -> list[str | None]:
    clock = [0.0]
    producer = FakeProducer(clock)

    def sleep(seconds: float) -> None:
        clock[0] += seconds

    run(
        _cfg(bad_rate=0.0, dup_rate=0.0),
        max_events=30,
        summary_file=None,
        sleep=sleep,
        now=lambda: clock[0],
        feed=FakeFeed(orders, clock),
        producer=producer,
        catalogue=CATALOGUE,
    )
    return [e.event.event_id for _, e, _ in producer.sent]


def test_same_seed_runs_over_different_orders_have_disjoint_event_ids() -> None:
    orders = _orders(3)
    renamed = [
        OrderRef(f"x{o.order_id}", o.customer_id, o.purchase_ts, o.product_ids) for o in orders
    ]
    first = _event_ids(orders)
    assert len(first) == 30
    assert len(set(first)) == 30
    assert set(first).isdisjoint(_event_ids(renamed))
    assert _event_ids(orders) == first


RECS = Recommendations((("p2", "toys"), ("p3", "books")), "3", "rerank")


def _run_feedback(
    recommend: Callable[[str], Recommendations | None], **overrides: Any
) -> tuple[dict[str, int], FakeProducer, list[str]]:
    clock = [0.0]
    producer = FakeProducer(clock)
    asked: list[str] = []

    def tracked(session_id: str) -> Recommendations | None:
        asked.append(session_id)
        return recommend(session_id)

    summary = run(
        _cfg(**overrides),
        max_events=40,
        summary_file=None,
        sleep=lambda s: clock.__setitem__(0, clock[0] + s),
        now=lambda: clock[0],
        feed=FakeFeed(_orders(6), clock),
        producer=producer,
        catalogue=CATALOGUE,
        recommend=tracked,
    )
    return summary, producer, asked


def _base(producer: FakeProducer) -> list[Emission]:
    return [e for _, e, _ in producer.sent if not e.event.event_type.startswith("recommendation")]


def test_feedback_follows_each_product_view_with_its_dataset_time() -> None:
    summary, producer, asked = _run_feedback(lambda _: RECS, bad_rate=0.0, dup_rate=0.0)
    views = [e for e in _base(producer) if e.event.event_type == "product_view"]
    assert len(asked) == len(views) > 0
    feedback = [
        (t, e, f) for t, e, f in producer.sent if e.event.event_type.startswith("recommendation")
    ]
    shown = [e for _, e, _ in feedback if e.event.event_type == "recommendation_shown"]
    assert len(shown) == 2 * len(views)
    assert summary["feedback"] == len(feedback)
    assert summary["recommend_failures"] == 0
    assert {e.schema_version for _, e, _ in feedback} == {3}
    trigger_of = {(v.event.session_id, v.event.event_ts): v for v in views}
    sent_at = {id(e): (t, f) for t, e, f in producer.sent}
    for t, emission, fallback in feedback:
        trigger = trigger_of[(emission.event.session_id, emission.event.event_ts)]
        assert emission.delay_s == trigger.delay_s
        assert (t, fallback) == sent_at[id(trigger)]


def test_recommend_failure_skips_feedback_and_the_sim_continues() -> None:
    plain, plain_producer = _run(_cfg(), max_events=40)
    summary, producer, asked = _run_feedback(lambda _: None)
    assert asked
    assert [e for e in producer.sent if not e[1].event.event_type.startswith("rec")] == (
        plain_producer.sent
    )
    assert summary["feedback"] == 0
    assert summary["recommend_failures"] == len(asked)
    assert summary["events_unique"] == plain["events_unique"] == 40


def test_feedback_off_by_default_asks_nothing() -> None:
    summary, producer = _run(_cfg(), max_events=40)
    assert (summary["feedback"], summary["recommend_failures"]) == (0, 0)
    assert all(not e.event.event_type.startswith("rec") for _, e, _ in producer.sent)


def test_config_from_env_defaults_and_overrides() -> None:
    env = {
        "KAFKA_BOOTSTRAP": "redpanda:9092",
        "SCHEMA_REGISTRY_URL": "http://redpanda:8081",
        "POSTGRES_DSN": "postgresql://u@h/db",
        "SIM_BAD_RATE": "0.05",
        "SIM_SCHEMA_EVOLVE_AFTER": "500",
        "REPLAY_SPEED": "600",
    }
    cfg = SimConfig.from_env(env)
    assert (cfg.late_rate, cfg.late_max_hours, cfg.dup_rate) == (0.05, 48, 0.01)
    assert (cfg.bad_rate, cfg.schema_evolve_after, cfg.browsing_ratio, cfg.seed) == (
        0.05,
        500,
        3,
        42,
    )
    assert cfg.speed == 600.0
    assert cfg.bootstrap == "redpanda:9092"
    assert cfg.recommend_url == "http://127.0.0.1:8000"
    assert SimConfig.from_env({"RECOMMEND_URL": "http://serving:8000"}).recommend_url == (
        "http://serving:8000"
    )


def test_run_feedback_flag_wires_a_recommend_client(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[object] = []

    def fake_run(cfg: SimConfig, **kwargs: Any) -> dict[str, int]:
        seen.append(kwargs.get("recommend"))
        return {"delivery_errors": 0, "unflushed": 0}

    monkeypatch.setattr(cli, "run", fake_run)
    assert cli.main(["run", "--max-events", "1"]) == 0
    assert cli.main(["run", "--max-events", "1", "--feedback"]) == 0
    assert seen[0] is None
    assert callable(seen[1])
