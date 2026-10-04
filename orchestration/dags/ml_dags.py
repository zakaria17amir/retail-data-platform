from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, TriggerRule
from common import GOLD, START, default_args
from ml_common import FEATURES, SCORES, ml_task, train_args


def ml_dag(dag_id: str, schedule: object) -> DAG:
    return DAG(
        dag_id,
        schedule=schedule,
        start_date=START,
        catchup=False,
        max_active_runs=1,
        default_args=default_args,
        tags=["ml"],
    )


with ml_dag("feast_materialize", [GOLD]):
    ml_task("materialize", ["materialize"], outlets=[FEATURES])

for model in ("late_delivery", "demand_forecast"):
    with ml_dag(f"train_{model}", "@weekly"):
        ml_task("train", train_args(model))

with ml_dag("score_late_delivery", "@daily"):
    ml_task("score", ["score", "late_delivery"], outlets=[SCORES])

# demand_daily comes from the same gold build that materialize was triggered by
with ml_dag("forecast_demand", [FEATURES]):
    ml_task("forecast", ["forecast", "demand"])

with ml_dag("monitor_late_delivery", [SCORES]):
    # exit 3 = drift/performance breach -> skipped; any other non-zero exit fails (and retries)
    monitor = ml_task("monitor", ["monitor", "late_delivery"], s3=True, skip_on_exit_code=3)
    monitor >> TriggerDagRunOperator(
        task_id="retrain",
        trigger_dag_id="train_late_delivery",
        trigger_rule=TriggerRule.ALL_SKIPPED,
    )
    # run state follows the leaves: this one fails the run when monitor fails
    monitor >> EmptyOperator(task_id="healthy")
