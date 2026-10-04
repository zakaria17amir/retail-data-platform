import os
from typing import Any

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import Asset
from docker.types import Mount

FEATURES = Asset("redis://redis:6379/0/feast")
SCORES = Asset("file:///opt/airflow/data/gold/ml/pred_late_delivery.parquet")
# optional overrides of the image's documented defaults, forwarded only when set for Airflow
PASSTHROUGH = (
    "GIT_SHA",
    "DRIFT_SHARE_THRESHOLD",
    "REFERENCE_SAMPLE_ROWS",
    "MIN_CURRENT_ROWS",
    "MIN_LABELLED",
    "MIN_POSITIVES",
)
S3_SECRETS = {
    "MINIO_ROOT_USER": "MINIO_ROOT_USER",
    "MINIO_ROOT_PASSWORD": "MINIO_ROOT_PASSWORD",
    "AWS_ACCESS_KEY_ID": "MINIO_ROOT_USER",
    "AWS_SECRET_ACCESS_KEY": "MINIO_ROOT_PASSWORD",
}


def train_args(model: str) -> list[str]:
    return ["train", model, "--promotion-mode", os.environ.get("PROMOTION_MODE") or "auto"]


def ml_task(task_id: str, args: list[str], s3: bool = False, **kwargs: Any) -> DockerOperator:
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
        mem_limit="2g",
        auto_remove="success",
        **kwargs,
    )
