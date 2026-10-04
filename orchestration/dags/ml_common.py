import logging
import os
from datetime import UTC, datetime
from typing import Any

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import Asset
from common import api_get, gold_is_due, send_alert
from docker.types import Mount

log = logging.getLogger(__name__)

FEATURES = Asset("redis://redis:6379/0/feast")
# monitor_late_delivery runs on it for a daily cadence after scoring; monitor itself no longer reads
# the file (it scores its own simulated-live window from the training Parquet)
SCORES = Asset("file:///opt/airflow/data/gold/ml/pred_late_delivery.parquet")
SESSIONS = Asset("file:///opt/airflow/data/gold/ml/sessions_offline.parquet")
# optional overrides of the image's documented defaults, forwarded only when set for Airflow
PASSTHROUGH = (
    "GIT_SHA",
    "DRIFT_SHARE_THRESHOLD",
    "REFERENCE_SAMPLE_ROWS",
    "MIN_CURRENT_ROWS",
    "MIN_LABELLED",
    "MIN_POSITIVES",
    "MONITOR_WINDOW_DAYS",
    "MONITOR_TAIL_RATIO",
)
S3_SECRETS = {
    "MINIO_ROOT_USER": "MINIO_ROOT_USER",
    "MINIO_ROOT_PASSWORD": "MINIO_ROOT_PASSWORD",
    "AWS_ACCESS_KEY_ID": "MINIO_ROOT_USER",
    "AWS_SECRET_ACCESS_KEY": "MINIO_ROOT_PASSWORD",
}


def train_args(model: str) -> list[str]:
    return ["train", model, "--promotion-mode", os.environ.get("PROMOTION_MODE") or "auto"]


def retrain_due() -> bool:
    # a breach persists until the model improves, so without a cooldown every daily monitor retrains
    if (os.environ.get("PROMOTION_MODE") or "auto") == "manual":
        log.info("PROMOTION_MODE=manual: breach left for a human, no retrain")
        return False
    hours = float(os.environ.get("RETRAIN_COOLDOWN_HOURS") or "72")
    last = None
    if hours > 0:
        found = api_get(
            "/api/v2/dags/train_late_delivery/dagRuns?state=success&order_by=-end_date&limit=1"
        )["dag_runs"]
        last = datetime.fromisoformat(found[0]["end_date"]) if found else None
    due = gold_is_due(last, datetime.now(UTC), hours)
    log.info("late_delivery last trained %s; cooldown %sh; retrain=%s", last, hours, due)
    return due


def breach_alert(ti: Any = None) -> None:
    try:
        send_alert(
            {"dag": ti.dag_id, "task": "monitor", "run_id": ti.run_id, "reason": "monitor breach"}
        )
    except Exception:
        log.exception("breach alert not sent")


def sessions_task(**kwargs: Any) -> DockerOperator:
    # offline session history for the recommender, from gold (no network needed)
    return DockerOperator(
        task_id="generate",
        image="retail-clickstream-sim",
        command=[
            "generate",
            "--gold-dir",
            "/data/gold",
            "--out",
            "/data/gold/ml/sessions_offline.parquet",
        ],
        mounts=[Mount(target="/data", source=f"{os.environ['HOST_REPO_DIR']}/data", type="bind")],
        mount_tmp_dir=False,
        mem_limit="2g",
        auto_remove="success",
        pool="ml",
        **kwargs,
    )


def ml_task(
    task_id: str, args: list[str], s3: bool = False, mem_limit: str = "2g", **kwargs: Any
) -> DockerOperator:
    env = {
        "MLFLOW_TRACKING_URI": "http://mlflow:5000",
        "REDIS_URL": "redis://redis:6379/0",
        "FEAST_REPO_PATH": "/feature_repo",
        "FEAST_REGISTRY_PATH": "/data/feast/registry.db",
        "GOLD_DIR": "/data/gold",
        "PROMOTION_MODE": os.environ.get("PROMOTION_MODE") or "auto",
        **{k: v for k in PASSTHROUGH if (v := os.environ.get(k))},
    }
    if s3:
        bucket = os.environ.get("LAKEHOUSE_BUCKET") or "lakehouse"
        env |= {
            "MINIO_ENDPOINT": "http://minio:9000",
            "AWS_ENDPOINT_URL": "http://minio:9000",
            "AWS_DEFAULT_REGION": "us-east-1",
            "MONITORING_REPORT_ROOT": os.environ.get("MONITORING_REPORT_ROOT")
            or f"s3://{bucket}/monitoring",
        }
    repo = os.environ["HOST_REPO_DIR"]
    # 1-slot pool: ML jobs write the feast file registry, which has no lock
    kwargs.setdefault("pool", "ml")
    return DockerOperator(
        task_id=task_id,
        image="retail-ml",
        command=args,
        network_mode="retail_retail",
        environment=env,
        private_environment={k: os.environ.get(v, "") for k, v in S3_SECRETS.items()} if s3 else {},
        # the Docker socket is the host's, so bind sources must be host paths
        mounts=[
            Mount(target="/data", source=f"{repo}/data", type="bind"),
            Mount(target="/feature_repo", source=f"{repo}/ml/feature_repo", type="bind"),
        ],
        mount_tmp_dir=False,
        mem_limit=mem_limit,
        auto_remove="success",
        **kwargs,
    )
