from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from common import SILVER, START, default_args, spark_task

with DAG(
    "silver_hourly",
    schedule="@hourly",
    start_date=START,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["silver"],
):
    quality = BashOperator(
        task_id="quality",
        bash_command='python -m lakehouse_quality.run run --root "s3://${LAKEHOUSE_BUCKET:-lakehouse}"',
        env={"PYTHONPATH": "/opt/airflow/lakehouse/quality/src"},
        append_env=True,
        outlets=[SILVER],
    )
    spark_task("silver", "silver/job.py", []) >> quality
