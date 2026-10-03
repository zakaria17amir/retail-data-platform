import argparse
import heapq
import itertools
import json
import os
import random
import signal
import sys
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from types import FrameType
from typing import Protocol

import psycopg
from dotenv import find_dotenv, load_dotenv

from clickstream_sim.cdc import OrderFeed
from clickstream_sim.events import Catalogue, OrderRef, browsing_session, converting_session
from clickstream_sim.faults import Emission, FaultConfig, inject
from clickstream_sim.producer import EventProducer

DEFAULT_DSN = "postgresql://retail:retail@127.0.0.1:5432/retail"
LATE_THRESHOLD_S = 3600
BROWSING_JITTER_S = 3600
IDLE_POLL_S = 1.0
MAX_WAIT_S = 0.1


class Feed(Protocol):
    def poll(self, timeout_s: float) -> OrderRef | None: ...


class Sink(Protocol):
    errors: int

    def produce(self, emission: Emission, fallback: datetime | None = None) -> None: ...

    def flush(self) -> int: ...


@dataclass(frozen=True)
class SimConfig:
    bootstrap: str
    registry_url: str
    postgres_dsn: str
    speed: float
    late_rate: float = 0.05
    late_max_hours: int = 48
    dup_rate: float = 0.01
    bad_rate: float = 0.0
    schema_evolve_after: int = 0
    browsing_ratio: int = 3
    seed: int = 42
    schemas_dir: Path = Path("ingestion/schemas/events")

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> "SimConfig":
        return cls(
            bootstrap=env.get("KAFKA_BOOTSTRAP", "127.0.0.1:19092"),
            registry_url=env.get("SCHEMA_REGISTRY_URL", "http://127.0.0.1:18081"),
            postgres_dsn=env.get("POSTGRES_DSN", DEFAULT_DSN),
            speed=float(env.get("REPLAY_SPEED", "3600")),
            late_rate=float(env.get("SIM_LATE_RATE", "0.05")),
            late_max_hours=int(env.get("SIM_LATE_MAX_HOURS", "48")),
            dup_rate=float(env.get("SIM_DUP_RATE", "0.01")),
            bad_rate=float(env.get("SIM_BAD_RATE", "0.0")),
            schema_evolve_after=int(env.get("SIM_SCHEMA_EVOLVE_AFTER", "0")),
            browsing_ratio=int(env.get("SIM_BROWSING_RATIO", "3")),
            seed=int(env.get("SIM_SEED", "42")),
            schemas_dir=Path(env.get("SIM_SCHEMAS_DIR", "ingestion/schemas/events")),
        )


def load_catalogue(dsn: str) -> Catalogue:
    with psycopg.connect(dsn) as conn:
        rows = conn.execute("SELECT product_id, product_category_name FROM olist.products")
        return Catalogue.from_rows(rows.fetchall())


def run(
    cfg: SimConfig,
    *,
    max_events: int | None,
    summary_file: Path | None,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], float] = time.monotonic,
    feed: Feed | None = None,
    producer: Sink | None = None,
    catalogue: Catalogue | None = None,
) -> dict[str, int]:
    feed = feed or OrderFeed(cfg.bootstrap, cfg.registry_url)
    producer = producer or EventProducer(cfg.bootstrap, cfg.registry_url, cfg.schemas_dir)
    catalogue = catalogue or load_catalogue(cfg.postgres_dsn)
    faults = FaultConfig(cfg.late_rate, cfg.late_max_hours, cfg.dup_rate, cfg.bad_rate)
    rng = random.Random(cfg.seed)

    stop = threading.Event()

    def request_stop(signum: int, frame: FrameType | None) -> None:
        stop.set()

    previous = (
        {sig: signal.signal(sig, request_stop) for sig in (signal.SIGINT, signal.SIGTERM)}
        if threading.current_thread() is threading.main_thread()
        else {}
    )

    heap: list[tuple[float, int, Emission, datetime, bool]] = []
    tie_breaker = itertools.count()
    unflushed = 0
    generated = 0
    counts = {"produced": 0, "duplicates": 0, "bad": 0, "late": 0, "schema_v2": 0}
    try:
        while not stop.is_set():
            generating = max_events is None or generated < max_events
            if not generating and not heap:
                break
            if generating:
                wait = min(max(heap[0][0] - now(), 0.0), MAX_WAIT_S) if heap else IDLE_POLL_S
                order = feed.poll(wait)
                if order is not None:
                    sessions = [converting_session(order, catalogue, rng)]
                    for _ in range(cfg.browsing_ratio):
                        jitter = timedelta(
                            seconds=rng.randint(-BROWSING_JITTER_S, BROWSING_JITTER_S)
                        )
                        sessions.append(
                            browsing_session(order.purchase_ts + jitter, catalogue, rng)
                        )
                    for event in (e for session in sessions for e in session):
                        if max_events is not None and generated >= max_events:
                            break
                        version = 2 if 0 < cfg.schema_evolve_after <= generated else 1
                        generated += 1
                        for emission in inject([event], faults, rng, version):
                            due = now() + emission.delay_s / cfg.speed
                            bad = emission.event is not event
                            heapq.heappush(
                                heap, (due, next(tie_breaker), emission, order.purchase_ts, bad)
                            )
            produced_now = False
            while heap and heap[0][0] <= now():
                _, _, emission, fallback, bad = heapq.heappop(heap)
                producer.produce(emission, fallback)
                produced_now = True
                counts["produced"] += 1
                if emission.is_duplicate:
                    counts["duplicates"] += 1
                    continue
                counts["bad"] += bad
                counts["late"] += emission.delay_s >= LATE_THRESHOLD_S
                counts["schema_v2"] += emission.schema_version == 2
            if not generating and not produced_now and heap and not stop.is_set():
                sleep(min(max(heap[0][0] - now(), 0.0), MAX_WAIT_S))
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
        unflushed = producer.flush()

    summary = {
        "events_unique": counts["produced"] - counts["duplicates"],
        "duplicates": counts["duplicates"],
        "bad": counts["bad"],
        "late": counts["late"],
        "produced": counts["produced"],
        "schema_v2": counts["schema_v2"],
        "delivery_errors": producer.errors,
        "unflushed": unflushed,
    }
    if summary_file is not None:
        summary_file.write_text(json.dumps(summary), encoding="utf-8")
    return summary


def report(summary: dict[str, int]) -> int:
    for key, value in summary.items():
        print(f"{key}: {value}")
    if summary["delivery_errors"] or summary["unflushed"]:
        print(
            f"error: {summary['delivery_errors']} delivery errors, "
            f"{summary['unflushed']} messages unflushed",
            file=sys.stderr,
        )
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    load_dotenv(find_dotenv(usecwd=True))
    parser = argparse.ArgumentParser(prog="clickstream-sim")
    commands = parser.add_subparsers(dest="command", required=True)
    run_parser = commands.add_parser("run", help="generate clickstream events from live orders")
    run_parser.add_argument("--max-events", type=int)
    run_parser.add_argument("--summary-file", type=Path)
    args = parser.parse_args(argv)
    try:
        cfg = SimConfig.from_env(os.environ)
        summary = run(cfg, max_events=args.max_events, summary_file=args.summary_file)
    except (ValueError, FileNotFoundError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except psycopg.OperationalError:
        print("error: could not connect to postgres", file=sys.stderr)
        return 1
    return report(summary)


if __name__ == "__main__":
    sys.exit(main())
