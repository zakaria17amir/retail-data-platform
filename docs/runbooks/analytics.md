# Analytics runbook (Phase 3, gold + orchestration)

Silver Delta → dbt-duckdb → gold star schema in `data/warehouse/retail.duckdb` and Parquet marts in
`data/gold/` (`GOLD_DIR`). Design: [ADR-0005](../adr/0005-gold-and-orchestration.md). Silver must exist
first ([lakehouse runbook](lakehouse.md)) and MinIO must be up. Run from the repo root in Git Bash.

## Build gold

```sh
make gold        # dbt deps + build --target local: models, data tests, unit tests
make dbt-parse   # manifests only: analytics/target/ (local, read by Airflow) + target-snowflake/
make sqlfluff
```

`make gold` creates the warehouse and `GOLD_DIR` directories. WARN results do not fail the build
(clickstream `at_least_one` before the first sim run, zip codes missing from `dim_geo`). DuckDB allows
one writer: do not run `make gold` while the Airflow `gold_daily` DAG is running.

## Query metrics (MetricFlow)

```sh
cd analytics
export PYTHONIOENCODING=utf-8   # Windows console: mf crashes on cp1252 without it
uv run mf validate-configs
uv run mf query --metrics revenue,orders,aov --group-by metric_time__month --order metric_time__month
uv run mf query --metrics late_delivery_rate,conversion_rate
```

## Airflow

Needs `AIRFLOW_ADMIN_PASSWORD`, `AIRFLOW_FERNET_KEY` (generator in `.env.example`) and, outside Git Bash,
`HOST_REPO_DIR` in `.env`. Run `make dbt-parse` (or `make gold`) first: `gold_daily` loads
`analytics/target/manifest.json` and `analytics/dbt_packages/` from the host.

```sh
make up PROFILE=analytics
```

UI at http://127.0.0.1:8088 (`AIRFLOW_PORT`), user `admin`, password `AIRFLOW_ADMIN_PASSWORD`.

DAGs start paused. Unpause the ones you want; asset events are not queued for a paused DAG, so unpause
`gold_daily` before `silver_hourly` publishes the silver asset:

```sh
make airflow-cli ARGS="dags unpause gold_daily"
make airflow-cli ARGS="dags unpause silver_hourly"     # hourly Spark silver + quality → silver asset
make airflow-cli ARGS="dags unpause ingest_health"     # every 15 min
make airflow-cli ARGS="dags trigger silver_hourly"     # run now; gold_daily follows on the asset
make airflow-cli ARGS="dags trigger gold_daily"        # gold only
make airflow-cli ARGS="dags list-runs gold_daily"
```

`lakehouse_maintenance` (weekly OPTIMIZE + VACUUM) runs Spark like `silver_hourly`; both use
DockerOperator over the host Docker socket, so the `retail-spark` image must be built.

## Alerts

Every failed task calls `on_failure`: with `ALERT_WEBHOOK_URL` set (Slack-compatible) it POSTs
`{"dag", "task", "run_id", "log_url"}`; without it, it logs `task failed (ALERT_WEBHOOK_URL unset)` in
the task log. Delivery errors are logged, never raised. `ingest_health` fails when a Kafka Connect
connector or task is not RUNNING or when the newest `bronze/olist/orders` `ingest_ts` is older than
`BRONZE_FRESHNESS_HOURS` (default 24). `log_url` points at `AIRFLOW__API__BASE_URL`
(`http://localhost:${AIRFLOW_PORT}`).

## Power BI

Open the PBIP project over `data/gold/*.parquet`: see [bi/README.md](../../bi/README.md).
