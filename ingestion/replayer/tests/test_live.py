import signal
from collections import Counter
from datetime import datetime
from pathlib import Path

import psycopg
import pytest
from replayer.live import apply_change, run_live
from replayer.load import seed
from replayer.schema import apply_schema
from replayer.timeline import Change, build_timeline

pytestmark = pytest.mark.integration

ORDER_CHANGE = Change(
    ts=datetime(2017, 1, 1),
    seq=0,
    table="orders",
    key=(("order_id", "oX"),),
    values=(("order_status", "created"),),
)


def _count(conn: psycopg.Connection, table: str) -> int:
    row = conn.execute(f"select count(*) from olist.{table}").fetchone()  # noqa: S608
    assert row is not None
    return int(row[0])


def test_apply_change_upsert_is_idempotent(conn: psycopg.Connection) -> None:
    apply_schema(conn)
    conn.execute("delete from olist.orders where order_id = 'oX'")
    apply_change(conn, ORDER_CHANGE)
    query = "select order_status, updated_at from olist.orders where order_id = 'oX'"
    first = conn.execute(query).fetchall()
    apply_change(conn, ORDER_CHANGE)
    second = conn.execute(query).fetchall()
    assert len(first) == len(second) == 1
    assert first[0][0] == second[0][0] == "created"
    assert second[0][1] > first[0][1]


def test_run_live_applies_all_changes_fast(dsn: str, tmp_data_dir: Path) -> None:
    sleeps: list[float] = []
    changes = build_timeline(tmp_data_dir, None, None)
    speed = 1e9
    counts = run_live(
        dsn, tmp_data_dir, start=None, until=None, speed=speed, loop=False, sleep=sleeps.append
    )
    assert counts == dict(Counter(c.table for c in changes))
    span = (changes[-1].ts - changes[0].ts).total_seconds()
    assert sum(sleeps) == pytest.approx(span / speed)
    assert all(s >= 0 for s in sleeps)


def test_run_live_twice_same_window_no_errors(dsn: str, tmp_data_dir: Path) -> None:
    kwargs = {"start": None, "until": None, "speed": 1e9, "loop": False, "sleep": lambda _: None}
    first = run_live(dsn, tmp_data_dir, **kwargs)  # type: ignore[arg-type]
    with psycopg.connect(dsn) as conn:
        rows = {t: _count(conn, t) for t in first}
    second = run_live(dsn, tmp_data_dir, **kwargs)  # type: ignore[arg-type]
    assert second == first
    with psycopg.connect(dsn) as conn:
        assert {t: _count(conn, t) for t in first} == rows


def test_reference_tables_loaded_once(dsn: str, tmp_data_dir: Path) -> None:
    kwargs = {"start": None, "until": None, "speed": 1e9, "loop": False, "sleep": lambda _: None}
    run_live(dsn, tmp_data_dir, **kwargs)  # type: ignore[arg-type]
    run_live(dsn, tmp_data_dir, **kwargs)  # type: ignore[arg-type]
    with psycopg.connect(dsn) as conn:
        assert _count(conn, "products") == 2
        assert _count(conn, "geolocation") == 2
        assert _count(conn, "sellers") == 1


def test_created_change_clears_seeded_later_columns(
    dsn: str, conn: psycopg.Connection, tmp_data_dir: Path
) -> None:
    seed(dsn, tmp_data_dir)
    created = next(
        c
        for c in build_timeline(tmp_data_dir, None, None)
        if c.table == "orders"
        and dict(c.key)["order_id"] == "o1"
        and dict(c.values)["order_status"] == "created"
    )
    apply_change(conn, created)
    row = conn.execute(
        "select order_status, order_approved_at, order_delivered_carrier_date, "
        "order_delivered_customer_date from olist.orders where order_id = 'o1'"
    ).fetchone()
    assert row == ("created", None, None, None)


def test_run_live_loop_shifts_history_until_signalled(
    dsn: str, tmp_data_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    changes = build_timeline(tmp_data_dir, None, None)
    applied: list[Change] = []

    def apply_then_terminate(connection: psycopg.Connection, change: Change) -> None:
        apply_change(connection, change)
        applied.append(change)
        if len(applied) == len(changes) + 5:
            signal.raise_signal(signal.SIGTERM)

    monkeypatch.setattr("replayer.live.apply_change", apply_then_terminate)
    counts = run_live(
        dsn, tmp_data_dir, start=None, until=None, speed=1e9, loop=True, sleep=lambda _: None
    )
    assert sum(counts.values()) == len(changes) + 5
    with psycopg.connect(dsn) as conn:
        row = conn.execute("select count(*) from olist.orders where order_id !~ '^o[0-9]$'")
        assert (row.fetchone() or (0,))[0] > 0
