import signal
import threading
import time
from collections import Counter
from collections.abc import Callable, Iterator
from datetime import datetime, timedelta
from pathlib import Path
from types import FrameType

import psycopg
from psycopg import sql

from replayer.load import copy_csv
from replayer.schema import TABLES, apply_schema
from replayer.timeline import REFERENCE_TABLES, Change, build_timeline, shift_history

MAX_SLEEP_SLICE = 0.5
SHIFT_CHUNK = 10_000


def apply_change(conn: psycopg.Connection, change: Change) -> None:
    pairs = (*change.key, *change.values)
    statement = sql.SQL(
        "INSERT INTO {table} ({columns}) VALUES ({placeholders}) "
        "ON CONFLICT ({keys}) DO UPDATE SET {updates}"
    ).format(
        table=sql.Identifier("olist", change.table),
        columns=sql.SQL(", ").join(sql.Identifier(c) for c, _ in pairs),
        placeholders=sql.SQL(", ").join(sql.Placeholder() for _ in pairs),
        keys=sql.SQL(", ").join(sql.Identifier(c) for c, _ in change.key),
        updates=sql.SQL(", ").join(
            sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c)) for c, _ in change.values
        ),
    )
    conn.execute(statement, [value for _, value in pairs])


def ensure_reference_tables(conn: psycopg.Connection, data_dir: Path) -> None:
    for table in TABLES:
        if table.name not in REFERENCE_TABLES:
            continue
        count = conn.execute(
            sql.SQL("SELECT count(*) FROM {t}").format(t=sql.Identifier("olist", table.name))
        ).fetchone()
        if count is not None and count[0] == 0:
            copy_csv(conn, table, data_dir / table.csv_file)


def _batch(timeline: list[Change], span: timedelta, iteration: int) -> Iterator[Change]:
    for offset in range(0, len(timeline), SHIFT_CHUNK):
        chunk = timeline[offset : offset + SHIFT_CHUNK]
        yield from chunk if iteration == 0 else shift_history(chunk, span, iteration)


def _interruptible_sleep(
    seconds: float, sleep: Callable[[float], None], stop: threading.Event
) -> None:
    while seconds > 0 and not stop.is_set():
        slice_ = min(seconds, MAX_SLEEP_SLICE)
        sleep(slice_)
        seconds -= slice_


def run_live(
    dsn: str,
    data_dir: Path,
    *,
    start: datetime | None,
    until: datetime | None,
    speed: float,
    loop: bool,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, int]:
    stop = threading.Event()

    def request_stop(signum: int, frame: FrameType | None) -> None:
        stop.set()

    previous = (
        {sig: signal.signal(sig, request_stop) for sig in (signal.SIGINT, signal.SIGTERM)}
        if threading.current_thread() is threading.main_thread()
        else {}
    )
    applied: Counter[str] = Counter()
    try:
        timeline = build_timeline(data_dir, start, until)
        with psycopg.connect(dsn, autocommit=True) as conn:
            apply_schema(conn)
            ensure_reference_tables(conn, data_dir)
            if timeline:
                span = timeline[-1].ts - timeline[0].ts + timedelta(days=1)
                iteration = 0
                previous_ts = timeline[0].ts
                while not stop.is_set():
                    for change in _batch(timeline, span, iteration):
                        _interruptible_sleep(
                            max((change.ts - previous_ts).total_seconds(), 0.0) / speed, sleep, stop
                        )
                        if stop.is_set():
                            break
                        apply_change(conn, change)
                        applied[change.table] += 1
                        previous_ts = max(previous_ts, change.ts)
                    iteration += 1
                    if not loop:
                        break
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
    return {t.name: applied[t.name] for t in TABLES if applied[t.name]}
