# ADR-0005: Gold on dbt-duckdb, MetricFlow metrics, Airflow 3 standalone orchestration

## Status
Accepted

## Context
Silver is Delta on MinIO. Gold must be a star schema plus reporting marts readable by Power BI and
Feast, keep a Snowflake path, define each metric once, and be scheduled with silver on a 16 GB laptop.

## Decision
- **Engine:** dbt-duckdb reads silver with `delta_scan('s3://…')` through a DuckDB S3 secret for MinIO.
  Marts are `external` Parquet at `<external_root>/<model>.parquet` (`external_root` = `GOLD_DIR` in
  `profiles.yml`; project-level `location` cannot use `this`). Packages: dbt_utils,
  metaplane/dbt_expectations (+ dbt_date), dbt-metricflow.
- **Snowflake parity:** marts are `table` there (target-type switch), dialect gaps via `adapter.dispatch`.
  dbt-snowflake would downgrade `certifi` workspace-wide, so `make dbt-parse` runs it in an ephemeral
  pinned `uv tool run` env writing `target-snowflake/` (`target/` stays the local manifest).
- **Metrics:** MetricFlow is the single definition. A revenue order is not `canceled`/`unavailable`, not
  deleted and has item revenue > 0 (also the AOV denominator).
- **SCD2 joins:** point-in-time on `valid_from`/`valid_to`, but a key's first version also matches earlier
  facts: CDC `valid_from` is processing time (2026), Olist facts are 2016-18.
- **Airflow 3.3 `standalone`**, one container: LocalExecutor, SimpleAuthManager (admin password from
  `.env`), parallelism 4 (each slot pre-forks a worker), Fernet key required. Production would split
  scheduler, API server, DAG processor and triggerer.
- **Spark tasks:** DockerOperator over the host Docker socket (root-equivalent, local dev only), host-path
  bind mounts via `HOST_REPO_DIR`.
- **dbt in Airflow:** Cosmos `DBT_MANIFEST` load (DAG parsing never runs dbt), dbt in its own venv;
  `gold_daily` runs on the silver asset with `max_active_tasks=1` (DuckDB has a single writer).
- **Packaging:** `orchestration/` is a separate uv project locked against Airflow's constraints; Airflow
  does not run on Windows, so DAG tests run in the container or on Linux CI. `exclude-newer` is an
  absolute date so `--locked` stays deterministic.
- **BI:** Power BI as PBIP/TMDL text, not a binary `.pbix`, so the model diffs in git.

## Consequences
- An empty external mart writes one all-NULL Parquet row (DuckDB's view filters it); Power BI shows it
  until clickstream arrives. `mf` on a Windows console needs `PYTHONIOENCODING=utf-8`.
- The Snowflake env is pinned only at top level and is parsed, never run.
- No Airflow SLAs in v3 (Airflow 3 dropped them); `ingest_health` and dbt freshness cover staleness.
- Rejected: dbt-delta plugin (`delta_scan` suffices), CeleryExecutor (more containers on one host).
