import json
import logging
import os
import urllib.request
from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.sdk import DAG, task
from common import START, default_args

log = logging.getLogger(__name__)


def unhealthy_connectors(status: dict[str, Any]) -> list[str]:
    if not status:
        return ["<no connectors>"]
    return [
        name
        for name, s in status.items()
        if s["status"]["connector"]["state"] != "RUNNING"
        or any(t["state"] != "RUNNING" for t in s["status"]["tasks"])
    ]


def bronze_is_fresh(latest: datetime | None, now: datetime, hours: float) -> bool:
    return latest is not None and now - latest <= timedelta(hours=hours)


with DAG(
    "ingest_health",
    schedule="*/15 * * * *",
    start_date=START,
    catchup=False,
    max_active_runs=1,
    default_args={**default_args, "retries": 0},  # the next 15-min run is the retry
    tags=["ingest"],
):

    @task
    def connectors_running() -> None:
        url = f"{os.environ['KAFKA_CONNECT_URL']}/connectors?expand=status"
        with urllib.request.urlopen(url, timeout=10) as resp:
            bad = unhealthy_connectors(json.load(resp))
        if bad:
            raise RuntimeError(f"connectors not RUNNING: {bad}")

    @task
    def bronze_fresh() -> None:
        import pyarrow.compute as pc
        from deltalake import DeltaTable

        bucket = os.environ.get("LAKEHOUSE_BUCKET", "lakehouse")
        table = DeltaTable(
            f"s3://{bucket}/bronze/olist/orders",
            storage_options={
                "AWS_ENDPOINT_URL": os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
                "AWS_ACCESS_KEY_ID": os.environ.get("MINIO_ROOT_USER", ""),
                "AWS_SECRET_ACCESS_KEY": os.environ.get("MINIO_ROOT_PASSWORD", ""),
                "AWS_ALLOW_HTTP": "true",
                "AWS_REGION": "us-east-1",
            },
        )
        latest = pc.max(table.to_pyarrow_table(columns=["ingest_ts"])["ingest_ts"]).as_py()
        hours = float(os.environ.get("BRONZE_FRESHNESS_HOURS", "24"))
        if not bronze_is_fresh(latest, datetime.now(UTC), hours):
            raise RuntimeError(f"bronze/olist/orders latest ingest_ts {latest} older than {hours}h")
        log.info("bronze/olist/orders latest ingest_ts %s", latest)

    connectors_running()
    bronze_fresh()
