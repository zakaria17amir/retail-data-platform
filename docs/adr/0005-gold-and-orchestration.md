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
  deleted and has item revenue > 0 (also the AOV denominator). Deleted orders are neither revenue nor
  delivered/late; deleted item/payment lines of a live order are excluded from the order totals and
  `fct_order_items.is_revenue_order`; facts keep deleted rows with `is_deleted`.
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
- Source freshness is warn-only (warn 6 h, no error) and off for the static reference tables
  (`categories`, `products`, `sellers`, `geolocation_points`, `zip_centroids`), which the replayer
  never reloads. `ingest_health` (`BRONZE_FRESHNESS_HOURS`) owns staleness alerts, so a paused replay
  never fails `gold_daily`.
- `_rule_metrics` has one row per silver micro-batch, not per run, so it has no grain test;
  `rpt_data_quality` sums the batches per run/table/rule.
- `gold_daily` short-circuits when its last success is less than `GOLD_MIN_INTERVAL_HOURS` (20 h) old,
  so hourly silver publications do not rebuild gold every hour; set it to 0 to force a rebuild.
- Cosmos runs with `should_detach_multiple_parents_tests=True`: a test with several parents runs once,
  after every parent is built.
- DuckDB mart views store the Parquet path of the last builder (host `../data/gold` vs container
  `/opt/airflow/data/gold`). Consumers read the Parquet files, never the DuckDB views; host `duckdb`
  queries of mart views break after an Airflow run until `make gold`.
- dbt-core pulls protobuf/pathspec downgrades into the workspace (accepted).
- Rejected: dbt-delta plugin (`delta_scan` suffices), CeleryExecutor (more containers on one host).
