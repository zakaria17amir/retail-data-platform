import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from clickstream_sim.cli import SimConfig, report, run
from clickstream_sim.events import Catalogue, OrderRef
from clickstream_sim.faults import Emission

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
