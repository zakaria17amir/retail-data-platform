import contextlib
import io
import json
import logging
import os
import sys
import urllib.error
import urllib.parse
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
TI = SimpleNamespace(dag_id="d", task_id="t", run_id="r", log_url="http://log")


@pytest.fixture(scope="module")
def manifest() -> Path:
    if not Path("/opt/airflow/analytics").exists():
        os.environ.setdefault("DBT_PROJECT_DIR", str(DAGS_DIR.parents[1] / "analytics"))
    path = Path(os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/analytics"))
    path /= "target/manifest.json"
    if not path.exists():
        msg = f"{path} missing: run `make dbt-parse` first"
        if os.environ.get("CI"):
            pytest.fail(msg)
        pytest.skip(msg)
    return path


@pytest.fixture(scope="module")
def dagbag(manifest: Path) -> DagBag:
    os.environ.setdefault("HOST_REPO_DIR", "/repo")
    return DagBag(dag_folder=str(DAGS_DIR))


def test_no_import_errors(dagbag: DagBag) -> None:
    assert dagbag.import_errors == {}


def test_dag_ids(dagbag: DagBag) -> None:
    assert set(dagbag.dag_ids) == DAG_IDS


def test_every_task_has_owner_and_retries(dagbag: DagBag) -> None:
    for dag in dagbag.dags.values():
        for t in dag.tasks:
            assert t.owner == "data-platform", (dag.dag_id, t.task_id)
            if dag.dag_id == "ingest_health":
                assert t.retries == 0, t.task_id
            else:
                assert t.retries >= 1, (dag.dag_id, t.task_id)


def test_gold_daily_is_scheduled_on_silver(dagbag: DagBag) -> None:
    dag = dagbag.dags["gold_daily"]
    assert dag.timetable.asset_condition == AssetAll(common.SILVER)
    assert any(common.GOLD in t.outlets for t in dag.tasks)
    assert dag.max_active_tasks == 1
    assert dag.max_active_runs == 1


def test_gold_daily_renders_models_and_source_checks(dagbag: DagBag) -> None:
    ids = set(dagbag.dags["gold_daily"].task_ids)
    assert {"dbt.stg_orders.run", "dbt.stg_orders.test", "dbt.silver_orders.source"} <= ids


def test_gold_daily_runs_multi_parent_tests_after_all_parents(dagbag: DagBag) -> None:
    dag = dagbag.dags["gold_daily"]
    (test,) = [t for t in dag.tasks if "assert_fct_orders_count_matches_stg_orders" in t.task_id]
    assert {"dbt.fct_orders.run", "dbt.stg_orders.run"} <= test.get_flat_relative_ids(upstream=True)


def test_gold_daily_gates_everything_on_should_build(dagbag: DagBag) -> None:
    dag = dagbag.dags["gold_daily"]
    gate = dag.get_task("should_build")
    assert [t.task_id for t in dag.roots] == ["should_build"]
    assert gate.get_flat_relative_ids(upstream=False) == set(dag.task_ids) - {"should_build"}


def test_gold_is_due() -> None:
    now = datetime(2026, 1, 2, tzinfo=UTC)
    assert common.gold_is_due(None, now, 20)
    assert common.gold_is_due(now - timedelta(hours=20), now, 20)
    assert not common.gold_is_due(now - timedelta(hours=19), now, 20)
    assert common.gold_is_due(now - timedelta(minutes=1), now, 0)


BUILT = {"run": "built", "end": "2026-10-04T04:02:31Z", "publish_gold": "success"}
# a short-circuited run: every leaf skipped, so the run itself ends in state success
SHORT_CIRCUITED = {"run": "skipped", "end": "2026-10-04T05:00:00Z", "publish_gold": "skipped"}


def fake_api(monkeypatch: pytest.MonkeyPatch, runs: list[dict[str, str]]) -> list[Any]:
    sent: list[Any] = []

    def urlopen(req: Any, timeout: float) -> Any:
        sent.append(req)
        url = urllib.parse.urlsplit(req.full_url)
        q = dict(urllib.parse.parse_qsl(url.query))
        if url.path == "/auth/token":
            body: Any = {"access_token": "tok"}
        elif url.path == "/api/v2/dags/gold_daily/dagRuns/~/taskInstances":
            assert q["order_by"] == "-end_date"
            rows = sorted(runs, key=lambda r: r["end"], reverse=True)
            rows = [r for r in rows if r[q["task_id"]] == q["state"]][: int(q["limit"])]
            body = {
                "task_instances": [{"dag_run_id": r["run"], "end_date": r["end"]} for r in rows]
            }
        else:
            raise AssertionError(req.full_url)
        return contextlib.nullcontext(io.BytesIO(json.dumps(body).encode()))

    monkeypatch.setenv("AIRFLOW_ADMIN_PASSWORD", "pw")
    monkeypatch.setattr(common.urllib.request, "urlopen", urlopen)
    return sent


def test_last_build_end_ignores_short_circuited_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    sent = fake_api(monkeypatch, [BUILT, SHORT_CIRCUITED])
    assert common.last_build_end() == datetime(2026, 10, 4, 4, 2, 31, tzinfo=UTC)
    login, tis = sent
    assert login.full_url == "http://localhost:8080/auth/token"
    assert json.loads(login.data) == {"username": "admin", "password": "pw"}
    assert tis.get_header("Authorization") == "Bearer tok"


def test_last_build_end_none_when_gold_never_published(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_api(monkeypatch, [SHORT_CIRCUITED])
    assert common.last_build_end() is None


REFERENCE_SOURCES = ("categories", "products", "sellers", "geolocation_points")


def test_sources_never_error_on_freshness(manifest: Path) -> None:
    # read at test time: `make dbt-parse` regenerates the manifest from analytics/
    sources = json.loads(manifest.read_text())["sources"]

    def count(name: str, key: str) -> Any:
        return ((sources[name].get("freshness") or {}).get(key) or {}).get("count")

    for name in REFERENCE_SOURCES:
        assert count(f"source.retail.silver.{name}", "warn_after") is None, name
    for name in sources:
        assert count(name, "error_after") is None, name


def test_silver_hourly_publishes_silver(dagbag: DagBag) -> None:
    dag = dagbag.dags["silver_hourly"]
    quality = dag.get_task("quality")
    assert common.SILVER in quality.outlets
    assert quality.upstream_task_ids == {"silver"}
    assert quality.bash_command == (
        'python -m lakehouse_quality.run run --root "s3://${LAKEHOUSE_BUCKET:-lakehouse}"'
    )


def test_spark_task_runs_on_compose_network(dagbag: DagBag) -> None:
    silver = dagbag.dags["silver_hourly"].get_task("silver")
    assert silver.image == "retail-spark"
    assert silver.network_mode == "retail_retail"
    assert silver.auto_remove == "success"
    assert silver.mounts[0]["Target"] == "/opt/lakehouse"
    assert silver.mounts[0]["ReadOnly"] is True


def test_spark_task_requires_host_repo_dir(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("HOST_REPO_DIR", raising=False)
    with pytest.raises(KeyError, match="HOST_REPO_DIR"):
        common.spark_task("x", "silver/job.py", [])


def test_failure_callback_posts_json(monkeypatch: pytest.MonkeyPatch) -> None:
    sent: list[Any] = []

    def urlopen(req: Any, timeout: float) -> contextlib.AbstractContextManager[None]:
        sent.append(req)
        return contextlib.nullcontext()

    monkeypatch.setenv("ALERT_WEBHOOK_URL", "http://hook.test/x")
    monkeypatch.setattr(common.urllib.request, "urlopen", urlopen)
    common.on_failure({"ti": TI})  # type: ignore[typeddict-item]
    (req,) = sent
    assert req.full_url == "http://hook.test/x"
    assert req.get_header("Content-type") == "application/json"
    assert json.loads(req.data) == {"dag": "d", "task": "t", "run_id": "r", "log_url": "http://log"}


def test_failure_callback_without_webhook_only_logs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    monkeypatch.setattr(common.urllib.request, "urlopen", pytest.fail)
    common.on_failure({"ti": TI})  # type: ignore[typeddict-item]


def test_failure_callback_swallows_webhook_errors(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def urlopen(req: Any, timeout: float) -> None:
        raise urllib.error.URLError("down")

    monkeypatch.setenv("ALERT_WEBHOOK_URL", "http://hook.test/x")
    monkeypatch.setattr(common.urllib.request, "urlopen", urlopen)
    with caplog.at_level(logging.ERROR, logger=common.log.name):
        common.on_failure({"ti": TI})  # type: ignore[typeddict-item]
    assert "failure alert not sent" in caplog.text


def test_failure_callback_log_url_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    sent: list[Any] = []
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "http://hook.test/x")
    monkeypatch.setenv("AIRFLOW__API__BASE_URL", "http://af:8080/")
    monkeypatch.setattr(
        common.urllib.request,
        "urlopen",
        lambda req, timeout: sent.append(req) or contextlib.nullcontext(),
    )
    ti = SimpleNamespace(dag_id="d", task_id="t", run_id="r")
    common.on_failure({"ti": ti})  # type: ignore[typeddict-item]
    assert json.loads(sent[0].data)["log_url"] == "http://af:8080/dags/d/runs/r/tasks/t"


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
