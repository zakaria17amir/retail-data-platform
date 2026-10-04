from airflow.sdk import DAG
from common import START, default_args, spark_task

with DAG(
    "lakehouse_maintenance",
    schedule="@weekly",
    start_date=START,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["silver"],
):
    spark_task("maintain", "silver/maintain.py", [])
