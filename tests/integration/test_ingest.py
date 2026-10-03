"""End-to-end ingest test: sample DB -> Debezium -> Redpanda -> Spark -> bronze Delta.

Destructive: reseeds the database with tests/fixtures/olist_sample, deletes the cdc/events topics
and clears bronze/ and the streaming checkpoints (ingestion/reset-bronze.sh). Requires
ALLOW_RESEED=1 and the ingest profile up. Restore afterwards with `make reset-bronze SEED=seed`.
"""

import csv
import json
import os
import subprocess
import time
from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.request import urlopen

import pytest
from deltalake import DeltaTable
from dotenv import find_dotenv, load_dotenv
from lakehouse_spark.cli import status_counts, storage_options
from replayer.schema import TABLES

load_dotenv(find_dotenv(usecwd=True))

pytestmark = pytest.mark.ingest

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "tests" / "fixtures" / "olist_sample"
CONNECT_URL = os.environ.get("KAFKA_CONNECT_URL", "http://127.0.0.1:8083")
BUCKET = os.environ.get("LAKEHOUSE_BUCKET", "lakehouse")
POLL_TIMEOUT_S = 240
POLL_INTERVAL_S = 5
EVENT_TYPES = ("add_to_cart", "checkout_started", "page_view", "product_view", "search")


def _connect_reachable() -> bool:
    try:
        with urlopen(f"{CONNECT_URL}/connectors", timeout=3):  # noqa: S310
            return True
    except OSError:
        return False


if os.environ.get("ALLOW_RESEED") != "1":
    pytest.skip("set ALLOW_RESEED=1 to reseed the database", allow_module_level=True)
if not _connect_reachable():
    pytest.skip("Kafka Connect is not reachable (make up PROFILE=ingest)", allow_module_level=True)


def _run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(  # noqa: S603
        command, cwd=ROOT, capture_output=True, text=True, **kwargs
    )
    assert result.returncode == 0, f"{command} failed:\n{result.stdout}\n{result.stderr}"
    return result


def _status() -> dict[str, Any]:
    return status_counts(f"s3://{BUCKET}", os.environ)


def _wait_until(what: str, predicate: Callable[[dict[str, Any]], bool]) -> dict[str, Any]:
    deadline = time.monotonic() + POLL_TIMEOUT_S
    status = _status()
    while not predicate(status):
        if time.monotonic() > deadline:
            logs = subprocess.run(  # noqa: S603
                ["docker", "compose", "--profile", "ingest", "logs", "--tail", "50", "spark"],
                cwd=ROOT,
                capture_output=True,
                text=True,
            )
            pytest.fail(
                f"timed out waiting for {what}\n{json.dumps(status, indent=1)}\n"
                f"--- spark logs ---\n{logs.stdout}"
            )
        time.sleep(POLL_INTERVAL_S)
        status = _status()
    return status


def _csv_rows(name: str) -> list[dict[str, str]]:
    with (FIXTURE / name).open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _events_total(status: dict[str, Any]) -> int:
    return int(sum(status[f"bronze/events/{t}"] for t in EVENT_TYPES) + status["quarantine/events"])


def _wait_for_consumer_group(group: str) -> None:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        described = subprocess.run(  # noqa: S603
            ["docker", "compose", "exec", "-T", "redpanda", "rpk", "group", "describe", group],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        if "Stable" in described.stdout:
            return
        time.sleep(2)
    pytest.fail(f"consumer group {group} did not become Stable")


def test_ingest_end_to_end(tmp_path: Path) -> None:
    snapshot = {f"bronze/olist/{t.name}": len(_csv_rows(t.csv_file)) for t in TABLES}

    _run(["sh", "ingestion/reset-bronze.sh", "seed-sample"], timeout=1500)

    status = _wait_until(
        "the snapshot to reach bronze", lambda s: all(s[k] == v for k, v in snapshot.items())
    )
    assert status["quarantine/olist"] == 0

    purchases = sorted(r["order_purchase_timestamp"] for r in _csv_rows("olist_orders_dataset.csv"))
    start = purchases[0].replace(" ", "T")
    until = (datetime.fromisoformat(purchases[-1]) + timedelta(seconds=1)).isoformat()
    sim_summary = tmp_path / "sim.json"
    replay_summary = tmp_path / "replay.json"
    sim_env = {
        **os.environ,
        "SIM_BAD_RATE": "0.1",
        "SIM_DUP_RATE": "0.1",
        "SIM_LATE_RATE": "0.2",
        "SIM_SCHEMA_EVOLVE_AFTER": "100",
        "REPLAY_SPEED": "1e6",
    }
    sim = subprocess.Popen(  # noqa: S603
        [
            "uv", "run", "--package", "clickstream-sim", "clickstream-sim", "run",
            "--max-events", "300", "--summary-file", str(sim_summary),
        ],
        cwd=ROOT,
        env=sim_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )  # fmt: skip
    try:
        _wait_for_consumer_group("clickstream-sim")
        _run(
            [
                "uv", "run", "--package", "replayer", "replayer", "live",
                "--data-dir", str(FIXTURE), "--from", start, "--until", until, "--speed", "1e6",
                "--summary-file", str(replay_summary),
            ],
            timeout=900,
        )  # fmt: skip
        live: dict[str, int] = json.loads(replay_summary.read_text())
        assert live, "replayer applied no changes"
        expected = {k: v + live.get(k.rsplit("/", 1)[-1], 0) for k, v in snapshot.items()}
        status = _wait_until(
            "live changes to reach bronze", lambda s: all(s[k] == v for k, v in expected.items())
        )
        assert status["quarantine/olist"] == 0

        output, _ = sim.communicate(timeout=POLL_TIMEOUT_S)
        assert sim.returncode == 0, output
    finally:
        if sim.poll() is None:
            sim.kill()

    summary: dict[str, int] = json.loads(sim_summary.read_text())
    unique, duplicates = summary["events_unique"], summary["duplicates"]
    assert unique == 300
    status = _wait_until("events to reach bronze", lambda s: _events_total(s) >= unique)
    time.sleep(POLL_INTERVAL_S * 2)
    status = _status()
    # only duplicates of events without an event_id (undeduplicable, all quarantined) can add rows
    assert unique <= _events_total(status) <= unique + duplicates, status

    reasons = status["quarantine/events reasons"]
    assert {"null_primary_key", "unparseable_timestamp"} <= set(reasons)

    options = storage_options(f"s3://{BUCKET}", os.environ)
    columns = {
        name
        for event_type in EVENT_TYPES
        if status[f"bronze/events/{event_type}"]
        for name in DeltaTable(f"s3://{BUCKET}/bronze/events/{event_type}", storage_options=options)
        .schema()
        .to_arrow()
        .names
    }
    assert "utm_campaign" in columns
