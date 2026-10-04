import json
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
sys.path.insert(0, str(DAGS_DIR))

import common  # noqa: E402
import ingest_health  # noqa: E402
from airflow.dag_processing.dagbag import DagBag  # noqa: E402
from airflow.sdk import AssetAll  # noqa: E402

DAG_IDS = {"ingest_health", "silver_hourly", "gold_daily", "lakehouse_maintenance"}


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    if not Path("/opt/airflow/analytics").exists():
        os.environ.setdefault("DBT_PROJECT_DIR", str(DAGS_DIR.parents[1] / "analytics"))
    return DagBag(dag_folder=str(DAGS_DIR))


def test_no_import_errors(dagbag: DagBag) -> None:
    assert dagbag.import_errors == {}


def test_dag_ids(dagbag: DagBag) -> None:
    assert set(dagbag.dag_ids) == DAG_IDS


def test_every_task_has_owner_and_retries(dagbag: DagBag) -> None:
    for dag in dagbag.dags.values():
        for t in dag.tasks:
            assert t.owner == "data-platform", (dag.dag_id, t.task_id)
            assert t.retries >= 1, (dag.dag_id, t.task_id)


def test_gold_daily_is_scheduled_on_silver(dagbag: DagBag) -> None:
    dag = dagbag.dags["gold_daily"]
    assert dag.timetable.asset_condition == AssetAll(common.SILVER)
    assert any(common.GOLD in t.outlets for t in dag.tasks)


def test_silver_hourly_publishes_silver(dagbag: DagBag) -> None:
    dag = dagbag.dags["silver_hourly"]
    quality = dag.get_task("quality")
    assert common.SILVER in quality.outlets
    assert quality.upstream_task_ids == {"silver"}


def test_spark_task_runs_on_compose_network(dagbag: DagBag) -> None:
    silver = dagbag.dags["silver_hourly"].get_task("silver")
    assert silver.image == "retail-spark"
    assert silver.network_mode == "retail_retail"
    assert silver.auto_remove == "success"
    assert silver.mounts[0]["Target"] == "/opt/lakehouse"
    assert silver.mounts[0]["ReadOnly"] is True


def test_failure_callback_posts_json(monkeypatch: pytest.MonkeyPatch) -> None:
    sent: list[Any] = []
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "http://hook.test/x")
    monkeypatch.setattr(common.urllib.request, "urlopen", lambda req, timeout: sent.append(req))
    ti = SimpleNamespace(dag_id="d", task_id="t", run_id="r", log_url="http://log")
    common.on_failure({"ti": ti})  # type: ignore[arg-type]
    (req,) = sent
    assert req.full_url == "http://hook.test/x"
    assert req.get_header("Content-type") == "application/json"
    assert json.loads(req.data) == {"dag": "d", "task": "t", "run_id": "r", "log_url": "http://log"}


def test_failure_callback_without_webhook_only_logs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    monkeypatch.setattr(common.urllib.request, "urlopen", pytest.fail)
    ti = SimpleNamespace(dag_id="d", task_id="t", run_id="r", log_url="http://log")
    common.on_failure({"ti": ti})  # type: ignore[arg-type]


def test_unhealthy_connectors() -> None:
    status = {
        "ok": {"status": {"connector": {"state": "RUNNING"}, "tasks": [{"state": "RUNNING"}]}},
        "bad_task": {"status": {"connector": {"state": "RUNNING"}, "tasks": [{"state": "FAILED"}]}},
        "paused": {"status": {"connector": {"state": "PAUSED"}, "tasks": []}},
    }
    assert ingest_health.unhealthy_connectors(status) == ["bad_task", "paused"]
    assert ingest_health.unhealthy_connectors({}) == ["<no connectors>"]


def test_bronze_is_fresh() -> None:
    now = datetime(2026, 1, 2, tzinfo=UTC)
    assert ingest_health.bronze_is_fresh(now - timedelta(hours=23), now, 24)
    assert not ingest_health.bronze_is_fresh(now - timedelta(hours=25), now, 24)
    assert not ingest_health.bronze_is_fresh(None, now, 24)
