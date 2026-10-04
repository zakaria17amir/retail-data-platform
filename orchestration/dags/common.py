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
