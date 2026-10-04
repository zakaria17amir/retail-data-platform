from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, TriggerRule, task
from common import GOLD, START, default_args
from ml_common import (
    FEATURES,
    SCORES,
    SESSIONS,
    breach_alert,
    ml_task,
    retrain_due,
    sessions_task,
    train_args,
)


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
    # breach path (monitor skipped): alert always, retrain unless in cooldown or manual promotion
    monitor >> task(trigger_rule=TriggerRule.ALL_SKIPPED)(breach_alert)()
    (
        monitor
        >> task.short_circuit(trigger_rule=TriggerRule.ALL_SKIPPED)(retrain_due)()
        >> TriggerDagRunOperator(task_id="retrain", trigger_dag_id="train_late_delivery")
    )
    # run state follows the leaves: this one fails the run when monitor fails
    monitor >> EmptyOperator(task_id="healthy")

with ml_dag("generate_sessions", [GOLD]):
    sessions_task(outlets=[SESSIONS])

# asset-triggered only, no weekly cron: the sessions are a deterministic function of gold, so a
# retrain without a new gold publish would refit the same data
with ml_dag("train_recommender", [SESSIONS]):
    # peak 1.98 GiB on the full history
    train = ml_task("train", train_args("recommender"), mem_limit="3g")
    train >> ml_task("publish", ["publish-candidates"])
