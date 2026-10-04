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
import ml_common  # noqa: E402
from airflow.dag_processing.dagbag import DagBag  # noqa: E402
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator  # noqa: E402
from airflow.sdk import AssetAll, TriggerRule  # noqa: E402

ML_DAG_IDS = {
    "feast_materialize",
    "train_late_delivery",
    "train_demand_forecast",
    "score_late_delivery",
    "forecast_demand",
    "monitor_late_delivery",
    "generate_sessions",
    "train_recommender",
}
DAG_IDS = {"ingest_health", "silver_hourly", "gold_daily", "lakehouse_maintenance"} | ML_DAG_IDS
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


def fake_api(
    monkeypatch: pytest.MonkeyPatch,
    runs: list[dict[str, str]],
    train_runs: list[dict[str, str]] | None = None,
) -> list[Any]:
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
        elif url.path == "/api/v2/dags/train_late_delivery/dagRuns":
            assert q["order_by"] == "-end_date"
            rows = sorted(train_runs or [], key=lambda r: r["end"], reverse=True)
            rows = [r for r in rows if r["state"] == q["state"]][: int(q["limit"])]
            body = {"dag_runs": [{"dag_run_id": r["run"], "end_date": r["end"]} for r in rows]}
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
    assert "alert not sent" in caplog.text


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


def test_gold_daily_builds_ml_models(dagbag: DagBag) -> None:
    ids = set(dagbag.dags["gold_daily"].task_ids)
    for model in ("ml_late_delivery_training", "ml_seller_features_daily", "ml_demand_daily"):
        assert f"dbt.{model}.run" in ids, model


def test_ml_dag_schedules(dagbag: DagBag) -> None:
    dags = dagbag.dags
    assert dags["feast_materialize"].timetable.asset_condition == AssetAll(common.GOLD)
    assert ml_common.FEATURES in dags["feast_materialize"].get_task("materialize").outlets
    # after materialize: one run per published gold, reading the gold it was triggered by
    assert dags["forecast_demand"].timetable.asset_condition == AssetAll(ml_common.FEATURES)
    assert dags["train_late_delivery"].timetable.expression == "0 0 * * 0"  # @weekly
    assert dags["train_demand_forecast"].timetable.expression == "0 0 * * 0"
    assert dags["score_late_delivery"].timetable.expression == "0 0 * * *"  # @daily
    assert ml_common.SCORES in dags["score_late_delivery"].get_task("score").outlets
    # daily cadence after scoring; monitor scores its own window and no longer reads the file
    assert dags["monitor_late_delivery"].timetable.asset_condition == AssetAll(ml_common.SCORES)
    # sessions are regenerated from each gold publish; the recommender retrains only on new sessions
    assert dags["generate_sessions"].timetable.asset_condition == AssetAll(common.GOLD)
    assert ml_common.SESSIONS in dags["generate_sessions"].get_task("generate").outlets
    assert dags["train_recommender"].timetable.asset_condition == AssetAll(ml_common.SESSIONS)
    for dag_id in ML_DAG_IDS:
        assert dags[dag_id].max_active_runs == 1, dag_id
        assert not dags[dag_id].catchup, dag_id


def test_ml_task_commands(dagbag: DagBag) -> None:
    def command(dag_id: str, task_id: str) -> list[str]:
        return list(dagbag.dags[dag_id].get_task(task_id).command)

    assert command("feast_materialize", "materialize") == ["materialize"]
    assert command("train_late_delivery", "train")[:2] == ["train", "late_delivery"]
    assert command("train_demand_forecast", "train")[:2] == ["train", "demand_forecast"]
    assert command("score_late_delivery", "score") == ["score", "late_delivery"]
    assert command("forecast_demand", "forecast") == ["forecast", "demand"]
    assert command("monitor_late_delivery", "monitor") == ["monitor", "late_delivery"]
    assert command("train_recommender", "train")[:2] == ["train", "recommender"]
    assert command("train_recommender", "publish") == ["publish-candidates"]


def test_recommender_publishes_after_train(dagbag: DagBag) -> None:
    dag = dagbag.dags["train_recommender"]
    assert dag.get_task("publish").upstream_task_ids == {"train"}
    # peak 1.98 GiB on the full history
    assert dag.get_task("train").mem_limit == "3g"


def test_generate_sessions_runs_the_simulator(dagbag: DagBag) -> None:
    t = dagbag.dags["generate_sessions"].get_task("generate")
    assert t.image == "retail-clickstream-sim"
    assert list(t.command) == [
        "generate",
        "--gold-dir",
        "/data/gold",
        "--out",
        "/data/gold/ml/sessions_offline.parquet",
    ]
    assert t.auto_remove == "success"
    assert t.mem_limit == "2g"
    (mount,) = t.mounts
    assert mount["Target"] == "/data"
    assert mount["Source"] == f"{os.environ['HOST_REPO_DIR']}/data"
    assert ml_common.SESSIONS.uri.endswith("/data/gold/ml/sessions_offline.parquet")


def test_ml_task_runs_retail_ml_image(dagbag: DagBag) -> None:
    for dag_id in ML_DAG_IDS - {"generate_sessions"}:
        for t in dagbag.dags[dag_id].tasks:
            if t.task_type != "DockerOperator":
                continue
            assert t.image == "retail-ml", t.task_id
            assert t.network_mode == "retail_retail"
            assert t.auto_remove == "success"
            if (dag_id, t.task_id) != ("train_recommender", "train"):
                assert t.mem_limit == "2g", (dag_id, t.task_id)
            assert t.environment["MLFLOW_TRACKING_URI"] == "http://mlflow:5000"
            assert t.environment["REDIS_URL"] == "redis://redis:6379/0"
            assert t.environment["FEAST_REPO_PATH"] == "/feature_repo"
            assert t.environment["FEAST_REGISTRY_PATH"] == "/data/feast/registry.db"
            assert t.environment["GOLD_DIR"] == "/data/gold"
            repo = os.environ["HOST_REPO_DIR"]
            mounts = {m["Target"]: m for m in t.mounts}
            assert mounts["/data"]["Source"] == f"{repo}/data"
            assert mounts["/data"]["ReadOnly"] is False
            assert mounts["/feature_repo"]["Source"] == f"{repo}/ml/feature_repo"


def test_only_monitor_gets_object_store_credentials(dagbag: DagBag) -> None:
    monitor = dagbag.dags["monitor_late_delivery"].get_task("monitor")
    assert set(monitor._private_environment) == {
        "MINIO_ROOT_USER",
        "MINIO_ROOT_PASSWORD",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
    }
    assert monitor.environment["MINIO_ENDPOINT"] == "http://minio:9000"
    assert monitor.environment["MONITORING_REPORT_ROOT"].startswith("s3://")
    assert not dagbag.dags["score_late_delivery"].get_task("score")._private_environment


def test_ml_task_forwards_only_set_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HOST_REPO_DIR", "/repo")
    monkeypatch.setenv("MONITOR_WINDOW_DAYS", "14")
    monkeypatch.setenv("MONITOR_TAIL_RATIO", "0.5")
    # compose passes unset overrides as empty strings; GIT_SHA unset -> the image's baked value
    monkeypatch.setenv("MIN_LABELLED", "")
    monkeypatch.delenv("GIT_SHA", raising=False)
    env = ml_common.ml_task("probe", ["monitor", "late_delivery"], s3=True).environment
    assert env["MONITOR_WINDOW_DAYS"] == "14"
    assert env["MONITOR_TAIL_RATIO"] == "0.5"
    assert "MIN_LABELLED" not in env
    assert "GIT_SHA" not in env


def test_monitor_breach_triggers_retraining(dagbag: DagBag) -> None:
    dag = dagbag.dags["monitor_late_delivery"]
    monitor = dag.get_task("monitor")
    # exit 3 (breach) -> skipped, no retries; any other non-zero exit -> failed
    assert list(monitor.skip_on_exit_code) == [3]
    # breach path: runs only when monitor skipped; skipped itself when monitor succeeds or fails
    for task_id in ("breach_alert", "retrain_due"):
        t = dag.get_task(task_id)
        assert t.upstream_task_ids == {"monitor"}, task_id
        assert t.trigger_rule == TriggerRule.ALL_SKIPPED, task_id
    retrain = dag.get_task("retrain")
    assert isinstance(retrain, TriggerDagRunOperator)
    assert retrain.trigger_dag_id == "train_late_delivery"
    assert retrain.trigger_dag_id in dagbag.dag_ids
    # short-circuited by retrain_due (cooldown / manual promotion)
    assert retrain.upstream_task_ids == {"retrain_due"}
    assert retrain.trigger_rule == TriggerRule.ALL_SUCCESS
    # run state comes from the leaves: a failed monitor must fail the run, not hide behind retrain
    healthy = dag.get_task("healthy")
    assert healthy.upstream_task_ids == {"monitor"}
    assert healthy.trigger_rule == TriggerRule.ALL_SUCCESS
    assert {t.task_id for t in dag.leaves} == {"breach_alert", "retrain", "healthy"}


def test_every_ml_docker_task_uses_ml_pool(dagbag: DagBag) -> None:
    # one ML container at a time: the feast file registry has no lock
    docker = [
        t for d in ML_DAG_IDS for t in dagbag.dags[d].tasks if t.task_type == "DockerOperator"
    ]
    assert len(docker) == 9
    for t in docker:
        assert t.pool == "ml", (t.dag_id, t.task_id)


def retrain_due(dagbag: DagBag) -> Any:
    return dagbag.dags["monitor_late_delivery"].get_task("retrain_due").python_callable


TRAINED_4D_AGO = {"run": "old", "end": "2026-10-01T00:00:00Z", "state": "success"}
TRAINED_1D_AGO = {"run": "new", "end": "2026-10-04T00:00:00Z", "state": "success"}
FAILED_TRAIN = {"run": "bad", "end": "2026-10-04T12:00:00Z", "state": "failed"}
NOW = datetime(2026, 10, 5, tzinfo=UTC)


@pytest.fixture
def frozen_now(monkeypatch: pytest.MonkeyPatch) -> None:
    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> "FrozenDatetime":
            return cls.fromtimestamp(NOW.timestamp(), tz)

    monkeypatch.setattr(ml_common, "datetime", FrozenDatetime)


def test_breach_retrains_when_due(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch, frozen_now: None
) -> None:
    monkeypatch.delenv("RETRAIN_COOLDOWN_HOURS", raising=False)  # default 72h
    monkeypatch.setenv("PROMOTION_MODE", "auto")
    sent = fake_api(monkeypatch, [], [TRAINED_4D_AGO, FAILED_TRAIN])
    assert retrain_due(dagbag)() is True
    login, runs = sent
    assert login.full_url == "http://localhost:8080/auth/token"
    assert runs.get_header("Authorization") == "Bearer tok"


def test_breach_retrains_when_never_trained(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch, frozen_now: None
) -> None:
    monkeypatch.setenv("PROMOTION_MODE", "auto")
    fake_api(monkeypatch, [], [FAILED_TRAIN])
    assert retrain_due(dagbag)() is True


def test_breach_within_cooldown_does_not_retrain(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch, frozen_now: None
) -> None:
    monkeypatch.delenv("RETRAIN_COOLDOWN_HOURS", raising=False)
    monkeypatch.setenv("PROMOTION_MODE", "auto")
    fake_api(monkeypatch, [], [TRAINED_4D_AGO, TRAINED_1D_AGO])
    assert retrain_due(dagbag)() is False
    monkeypatch.setenv("RETRAIN_COOLDOWN_HOURS", "12")
    assert retrain_due(dagbag)() is True


def test_breach_with_manual_promotion_does_not_retrain(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PROMOTION_MODE", "manual")
    monkeypatch.setattr(common.urllib.request, "urlopen", pytest.fail)
    assert retrain_due(dagbag)() is False


def breach_alert(dagbag: DagBag) -> Any:
    return dagbag.dags["monitor_late_delivery"].get_task("breach_alert").python_callable


def test_breach_alert_posts_json(dagbag: DagBag, monkeypatch: pytest.MonkeyPatch) -> None:
    sent: list[Any] = []
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "http://hook.test/x")
    monkeypatch.setattr(
        common.urllib.request,
        "urlopen",
        lambda req, timeout: sent.append(req) or contextlib.nullcontext(),
    )
    ti = SimpleNamespace(dag_id="monitor_late_delivery", task_id="breach_alert", run_id="r")
    breach_alert(dagbag)(ti=ti)
    (req,) = sent
    assert req.full_url == "http://hook.test/x"
    assert json.loads(req.data) == {
        "dag": "monitor_late_delivery",
        "task": "monitor",
        "run_id": "r",
        "reason": "monitor breach",
    }


def test_breach_alert_never_raises(dagbag: DagBag, monkeypatch: pytest.MonkeyPatch) -> None:
    def urlopen(req: Any, timeout: float) -> None:
        raise urllib.error.URLError("down")

    ti = SimpleNamespace(dag_id="d", task_id="breach_alert", run_id="r")
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "http://hook.test/x")
    monkeypatch.setattr(common.urllib.request, "urlopen", urlopen)
    breach_alert(dagbag)(ti=ti)
    monkeypatch.delenv("ALERT_WEBHOOK_URL")
    monkeypatch.setattr(common.urllib.request, "urlopen", pytest.fail)
    breach_alert(dagbag)(ti=ti)
    breach_alert(dagbag)(ti=None)


@pytest.mark.parametrize("mode", ["auto", "manual"])
def test_train_passes_promotion_mode(
    monkeypatch: pytest.MonkeyPatch, manifest: Path, mode: str
) -> None:
    monkeypatch.setenv("HOST_REPO_DIR", "/repo")
    monkeypatch.setenv("PROMOTION_MODE", mode)
    bag = DagBag(dag_folder=str(DAGS_DIR / "ml_dags.py"))
    for model in ("late_delivery", "demand_forecast", "recommender"):
        train = bag.dags[f"train_{model}"].get_task("train")
        assert list(train.command) == ["train", model, "--promotion-mode", mode]


def test_promotion_mode_defaults_to_auto(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PROMOTION_MODE", raising=False)
    assert ml_common.train_args("late_delivery") == [
        "train",
        "late_delivery",
        "--promotion-mode",
        "auto",
    ]


def test_ml_task_requires_host_repo_dir(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("HOST_REPO_DIR", raising=False)
    with pytest.raises(KeyError, match="HOST_REPO_DIR"):
        ml_common.ml_task("x", ["materialize"])
