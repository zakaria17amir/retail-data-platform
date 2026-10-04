import json
import logging
import os
import urllib.request
from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import Asset, Context
from docker.types import Mount

log = logging.getLogger(__name__)

SILVER = Asset("s3://lakehouse/silver")
GOLD = Asset("file:///opt/airflow/data/gold")
START = datetime(2026, 10, 1, tzinfo=UTC)


def on_failure(context: Context) -> None:
    try:
        ti = context["ti"]
        base = os.environ.get("AIRFLOW__API__BASE_URL", "http://localhost:8080").rstrip("/")
        log_url = getattr(ti, "log_url", None) or (
            f"{base}/dags/{ti.dag_id}/runs/{ti.run_id}/tasks/{ti.task_id}"
        )
        payload = {"dag": ti.dag_id, "task": ti.task_id, "run_id": ti.run_id, "log_url": log_url}
        url = os.environ.get("ALERT_WEBHOOK_URL")
        if not url:
            log.error("task failed (ALERT_WEBHOOK_URL unset): %s", json.dumps(payload))
            return
        req = urllib.request.Request(
            url, data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=10):
            log.info("failure alert sent: %s", json.dumps(payload))
    except Exception:
        log.exception("failure alert not sent")


def gold_is_due(last_end: datetime | None, now: datetime, min_hours: float) -> bool:
    return min_hours <= 0 or last_end is None or now - last_end >= timedelta(hours=min_hours)


def last_build_end(api: str = "http://localhost:8080") -> datetime | None:
    # asset-triggered runs have no logical_date, so the Task SDK's get_previous_dagrun and
    # prev_end_date_success are always None; ask the REST API (served in this container) instead.
    # A short-circuited run also ends in success, so look for the last publish_gold that succeeded.
    login = urllib.request.Request(
        f"{api}/auth/token",
        data=json.dumps(
            {"username": "admin", "password": os.environ["AIRFLOW_ADMIN_PASSWORD"]}
        ).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(login, timeout=10) as resp:
        token = json.load(resp)["access_token"]
    tis = urllib.request.Request(
        f"{api}/api/v2/dags/gold_daily/dagRuns/~/taskInstances"
        "?task_id=publish_gold&state=success&order_by=-end_date&limit=1",
        headers={"Authorization": f"Bearer {token}"},
    )
    with urllib.request.urlopen(tis, timeout=10) as resp:
        found = json.load(resp)["task_instances"]
    return datetime.fromisoformat(found[0]["end_date"]) if found else None


default_args: dict[str, Any] = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure,
}


def spark_task(task_id: str, script: str, args: list[str]) -> DockerOperator:
    return DockerOperator(
        task_id=task_id,
        image="retail-spark",
        command=["spark-submit", f"/opt/lakehouse/src/lakehouse_spark/{script}", *args],
        network_mode="retail_retail",
        environment={"LAKEHOUSE_BUCKET": os.environ.get("LAKEHOUSE_BUCKET", "lakehouse")},
        private_environment={
            k: os.environ.get(k, "") for k in ("MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD")
        },
        # the Docker socket is the host's, so bind sources must be host paths
        mounts=[
            Mount(
                target="/opt/lakehouse",
                source=f"{os.environ['HOST_REPO_DIR']}/lakehouse/spark",
                type="bind",
                read_only=True,
            )
        ],
        mount_tmp_dir=False,
        mem_limit="3g",
        auto_remove="success",
    )
