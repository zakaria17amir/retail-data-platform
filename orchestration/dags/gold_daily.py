import os
from pathlib import Path

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG
from common import GOLD, SILVER, START, default_args
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import InvocationMode, LoadMode, SourceRenderingBehavior

PROJECT = Path(os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/analytics"))

with DAG(
    "gold_daily",
    schedule=[SILVER],
    start_date=START,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,  # DuckDB allows a single writer
    default_args=default_args,
    tags=["gold"],
):
    # manifest and dbt_packages come from `make dbt-parse` / `make gold` on the host (analytics/ is
    # mounted), so parsing this DAG never runs dbt
    dbt = DbtTaskGroup(
        group_id="dbt",
        project_config=ProjectConfig(
            PROJECT,
            manifest_path=PROJECT / "target" / "manifest.json",
            install_dbt_deps=False,
        ),
        profile_config=ProfileConfig(
            profile_name="retail",
            target_name="local",
            profiles_yml_filepath=PROJECT / "profiles.yml",
        ),
        execution_config=ExecutionConfig(dbt_executable_path="/opt/dbt-venv/bin/dbt"),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            # Cosmos' default DBT_RUNNER mode requires dbt inside Airflow's own environment
            invocation_mode=InvocationMode.SUBPROCESS,
            source_rendering_behavior=SourceRenderingBehavior.WITH_TESTS_OR_FRESHNESS,
            emit_datasets=False,
        ),
    )
    dbt >> EmptyOperator(task_id="publish_gold", outlets=[GOLD])
