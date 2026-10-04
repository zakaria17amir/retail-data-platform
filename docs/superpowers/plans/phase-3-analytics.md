# Phase 3 — Analytics (Gold), Orchestration, BI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A multi-target dbt project builds a Kimball star schema and reporting marts from silver Delta
(DuckDB locally, Snowflake-ready), governed metrics live in the dbt semantic layer, marts land as
Parquet that Power BI reads, and Airflow 3 runs silver → quality → gold on dataset-aware schedules.

**Architecture:** dbt-duckdb reads silver with `delta_scan` from MinIO, builds staging views,
intermediate tables and marts in `data/warehouse/retail.duckdb`; marts are materialised `external`
to `data/gold/<mart>.parquet`. MetricFlow semantic models define revenue, AOV, conversion and
late-delivery rate once. One Airflow container (`airflow standalone`, LocalExecutor, metadata in the
existing Postgres) runs four DAGs: `ingest_health`, `silver_hourly` (Spark silver + quality via the
Docker socket, publishes asset `silver`), `gold_daily` (Cosmos `dbt build`, triggered by `silver`,
publishes `gold`), `lakehouse_maintenance`. Power BI ships as a PBIP project (TMDL semantic model)
over the Parquet folder.

**Tech Stack:** dbt-core 1.x + dbt-duckdb + dbt-snowflake, DuckDB `delta` + `httpfs` extensions,
MetricFlow (`dbt-metricflow`), dbt_utils, dbt-expectations (maintained fork), sqlfluff (dbt
templater), Apache Airflow 3.x, astronomer-cosmos, Docker provider, Power BI Desktop (PBIP/TMDL).

**Spec:** `docs/superpowers/specs/retail-data-platform-design.md` §7 (and §6 silver contract in
`docs/superpowers/plans/phase-2-lakehouse.md`, "Silver contract").

## Global Constraints

- Python 3.12 uv; `analytics/` is a uv workspace member (package `analytics`, deps only). Airflow is a
  **separate uv project** at `orchestration/` (own `pyproject.toml` + `uv.lock`, not a workspace member)
  because Airflow's constraint set conflicts with the workspace (GX/pandas 3); its tests run with
  `uv run --project orchestration pytest`.
- Every dependency pinned to a release published ≥ 7 days ago; images `tag@sha256`; memory limits;
  host ports on `127.0.0.1`.
- dbt conventions: staging = renames/casts only, one view per silver table; marts `fct_`/`dim_`/`rpt_`;
  every model has a description; every key `unique` + `not_null`; every FK `relationships`; dialect
  differences only in `dispatch` macros; models compile on `local` and `snowflake`.
- `sqlfluff lint analytics/models` clean (dialect duckdb, dbt templater).
- Revenue is defined once: `sum(price + freight_value)` over order items of orders whose
  `order_status not in ('canceled', 'unavailable')`. AOV = revenue / distinct such orders. Conversion =
  sessions with a `checkout_started` event / all sessions. Late-delivery rate = delivered orders with
  `order_delivered_customer_ts_utc > order_estimated_delivery_ts_utc` / delivered orders.
- Gold Parquet at `${GOLD_DIR:-data/gold}/<model>.parquet` (gitignored); DuckDB file
  `${DUCKDB_PATH:-data/warehouse/retail.duckdb}`. Only one dbt writer at a time.
- Point-in-time dims: facts join SCD2 dims on `natural key` + `fact_ts_utc ∈ [valid_from, valid_to)`
  (open `valid_to` = null); surrogate key `md5(natural_key || valid_from)` via `dbt_utils.generate_surrogate_key`.
- Conventional commits; parallel tasks in separate git worktrees.

## Review Focus

1. **Fact rows lost by the SCD2 join** (a fact timestamp before a key's first `valid_from`, or a
   deleted dim row): every fact row must still join (snapshot versions start 1900-01-01; deleted versions
   are kept). Test: `fct_orders` row count == silver orders row count. → T2.
2. **Double counting from items × payments** (orders have several items and several payments):
   revenue must come from items only, payments aggregated separately. Unit test on an order with 2 items
   and 2 payments. → T2.
3. **Canceled/unavailable orders** excluded from revenue and AOV but present in `fct_orders`. → T2, T3.
4. **Sessions without events of some type / events without customer**: `fct_sessions` keeps anonymous
   sessions; conversion denominator counts them. → T2.
5. **Silver table missing or empty** on a fresh stack: `dbt build` fails with a clear source error, not a
   silent empty mart; `gold_daily` only runs after `silver` is published. → T1 (source freshness/tests), T5.

---

### Task 1: dbt project scaffold, profiles, sources, staging

**Owner:** `analytics-engineer`. Exception: Makefile targets `gold`, `dbt-parse`; root `pyproject.toml`
members gains `analytics`; `.gitignore` adds `data/gold/`, `data/warehouse/`, `analytics/target/`,
`analytics/dbt_packages/`, `analytics/logs/`; `.env.example` gains `GOLD_DIR`, `DUCKDB_PATH`,
`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`,
`SNOWFLAKE_DATABASE` (empty defaults).

**Files:** `analytics/pyproject.toml`, `analytics/dbt_project.yml`, `analytics/profiles.yml`,
`analytics/packages.yml`, `analytics/.sqlfluff`, `analytics/models/staging/_sources.yml`,
`analytics/models/staging/stg_*.sql` (+ `_staging.yml`), `analytics/macros/` (dispatch macros as needed).

**Interfaces (produced):**
- profile `retail`, targets `local` (duckdb at `DUCKDB_PATH`, extensions `delta`, `httpfs`, an S3
  secret for MinIO: endpoint from `MINIO_ENDPOINT` host part, `URL_STYLE 'path'`, `USE_SSL false`,
  keys from `MINIO_ROOT_*`) and `snowflake` (all from `SNOWFLAKE_*` env with `env_var(..., '')`).
- source `silver` with tables `categories, products, customers, sellers, geolocation_points,
  zip_centroids, orders, order_items, order_payments, order_reviews, clickstream`; on duckdb each has
  `external_location: "delta_scan('s3://{{ env_var('LAKEHOUSE_BUCKET','lakehouse') }}/silver/<domain>/<table>')"`
  (exact paths from the silver contract). If `delta_scan` cannot read MinIO, use dbt-duckdb's `delta`
  plugin (deltalake) instead and record why.
- staging models `stg_<table>` (11), views, column renames only (drop `_run_id`; keep `_is_deleted`,
  SCD2 columns, `_source_lsn`).
- Make: `gold` = `cd analytics && uv run dbt deps && uv run dbt build --target local`;
  `dbt-parse` = `dbt parse` for `local` and `snowflake`.

- [ ] **Step 1:** scaffold + `dbt debug --target local` OK against the dev stack (silver populated by Phase 2).
- [ ] **Step 2:** staging + source tests (`not_null`/`unique` on keys of current rows via `where`), source
  freshness on `_silver_loaded_at` (warn 6 h, error 48 h). `dbt build --select staging` green; paste counts.
- [ ] **Step 3:** `dbt parse --target snowflake` succeeds with empty env; `sqlfluff lint` clean.
- [ ] **Step 4:** Commit `feat(analytics): dbt project, multi-target profiles, silver sources and staging`.

---

### Task 2: intermediate models, facts, dims, unit tests

**Owner:** `analytics-engineer`. Depends on T1.

**Models:**
- `int_order_lifecycle` (one row per order: purchase→approve, approve→carrier, carrier→customer,
  purchase→customer hours; `is_late`, `is_delivered`), `int_sessions` (one row per `session_id`:
  start/end, event counts by type, `has_checkout`, `customer_id` if any, `order_id` if any),
  `int_customer_orders` (order sequence number per `customer_unique_id`, `is_first_order`).
- dims: `dim_date` (dbt_utils.date_spine 2016-01-01..2019-12-31), `dim_customer` (SCD2 from
  `stg_customers`, `customer_sk`), `dim_product` (SCD2, `product_sk`, English category), `dim_seller`
  (SCD2, `seller_sk`), `dim_geo` (zip centroids), `dim_payment_type`.
- facts: `fct_orders` (grain order; `customer_sk` point-in-time; lifecycle measures; status; revenue
  flag `is_revenue_order`), `fct_order_items` (grain item; `product_sk`, `seller_sk` point-in-time;
  `price`, `freight_value`, `item_revenue = price + freight_value`), `fct_payments`, `fct_reviews`,
  `fct_sessions` (from `int_sessions`), `fct_events` (daily aggregate by `event_date`, `event_type`).
- tests: generic key/FK tests on every fact/dim; dbt unit tests for the three intermediate models
  (Review Focus 2, 4 cases included) and for the point-in-time join (Review Focus 1: fact before first
  version, after a change, deleted dim row); singular test `fct_orders` count == `stg_orders` count.

- [ ] Steps: write unit tests first (`dbt test --select test_type:unit` fails), implement, `dbt build
  --exclude rpt_*` green on dev data, `sqlfluff lint` clean, `dbt parse --target snowflake` OK.
- [ ] Commit `feat(analytics): intermediate models, star schema facts and scd2 dims`.

---

### Task 3: reporting marts, metrics, expectations

**Owner:** `analytics-engineer`. Depends on T2.

- marts (external Parquet on duckdb, tables with `cluster_by` on snowflake, via `dbt_project.yml`
  config `materialized: "{{ 'external' if target.type == 'duckdb' else 'table' }}"` and `location`
  `{{ env_var('GOLD_DIR','../data/gold') }}/{{ this.name }}.parquet`): `rpt_daily_sales` (date × state:
  orders, revenue, AOV), `rpt_customer_360` (grain `customer_unique_id`: recency/frequency/monetary,
  RFM scores 1-5 by ntile, LTV = total revenue, first/last order, orders), `rpt_product_performance`
  (product × month: units, revenue, avg review, late rate), `rpt_funnel` (date: sessions, product_view,
  add_to_cart, checkout sessions, conversion), `rpt_delivery_sla` (month × customer_state: delivered,
  late, late rate, avg delivery hours), `rpt_data_quality` (from silver `_rule_metrics` and `_dq_results`
  via two extra sources: per run/table/rule rejects and pct; per run/check success).
  Also export the star schema facts/dims to Parquet (same config) so Power BI can model them.
- `analytics/models/metrics/`: semantic models on `fct_order_items`, `fct_orders`, `fct_sessions`;
  metrics `revenue`, `orders`, `aov` (ratio), `conversion_rate` (ratio), `late_delivery_rate` (ratio) with
  the exact definitions in Global Constraints. `mf validate-configs` passes; `mf query --metrics revenue
  --group-by metric_time__month` returns rows on dev data.
- dbt-expectations tests: revenue ≥ 0, conversion ∈ [0, 1], late rate ∈ [0, 1], RFM scores ∈ 1..5,
  `rpt_daily_sales` revenue sum == `fct_order_items` revenue sum of revenue orders (singular test).
- [ ] Steps: tests first, implement, `make gold` green; list `data/gold/*.parquet` with row counts
  (duckdb `read_parquet`); paste `mf query` output.
- [ ] Commit `feat(analytics): reporting marts as parquet, semantic layer metrics and expectations`.

---

### Task 4: Airflow container and compose profile `analytics`

**Owner:** `platform-engineer`. Independent of T1-T3 (wave 1 with T1).

**Files:** `orchestration/Dockerfile`, `orchestration/pyproject.toml` (+ `uv.lock`), compose services,
Makefile targets, `.env.example` (`AIRFLOW_PORT=8088`, `AIRFLOW_ADMIN_PASSWORD=` with a note,
`ALERT_WEBHOOK_URL=`).

- image `FROM apache/airflow:<3.x>-python3.12@sha256:…` + `astronomer-cosmos`,
  `apache-airflow-providers-docker`, the `lakehouse-quality` deps (`deltalake`, `great-expectations`
  pinned like the workspace) and a separate venv `/opt/dbt-venv` with dbt-duckdb (+ the T1 versions) so
  Cosmos runs `dbt` there. Pin using Airflow's constraints file for the chosen version.
- compose (profile `analytics`; postgres/minio add `analytics` to their profiles): `airflow-init`
  (creates database `airflow` in the existing Postgres if missing, `airflow db migrate`, admin user from
  env), `airflow` (`airflow standalone`, `AIRFLOW__CORE__EXECUTOR=LocalExecutor`, metadata
  `postgresql+psycopg2://…@postgres:5432/airflow`, `AIRFLOW__CORE__LOAD_EXAMPLES=false`, mounts
  `./orchestration/dags`, `./analytics`, `./data`, `./lakehouse`, `/var/run/docker.sock`; UI on
  `127.0.0.1:${AIRFLOW_PORT}`; healthcheck on the API `/api/v2/monitor/health`; limit 2 g).
- Make: `up PROFILE=analytics` works via the existing init loop; `airflow-cli` helper
  (`docker compose exec airflow airflow $(ARGS)`).
- [ ] Verify: `make up PROFILE=analytics` exit 0; UI login; `airflow dags list` runs (no DAGs yet);
  `docker compose exec airflow docker ps` works (socket); `/opt/dbt-venv/bin/dbt --version`.
- [ ] Commit `feat(platform): airflow 3 container and analytics profile`.

---

### Task 5: Airflow DAGs and DAG tests

**Owner:** `data-engineer`. Depends on T1 (dbt project path), T4.

**Files:** `orchestration/dags/{common,ingest_health,silver_hourly,gold_daily,lakehouse_maintenance}.py`,
`orchestration/tests/test_dags.py`.

- `common.py`: `default_args` (owner `data-platform`, retries 2, retry_delay 5 min), `on_failure`
  callback posting `{"dag", "task", "run_id", "log_url"}` JSON to `ALERT_WEBHOOK_URL` if set (else log),
  assets `SILVER = Asset("s3://lakehouse/silver")`, `GOLD = Asset("file:///opt/airflow/data/gold")`, a
  `spark_task(task_id, script, args)` helper building a `DockerOperator` (image `retail-spark`, network
  `retail_retail` — verify the real network name, env from Airflow env, `auto_remove="success"`, mount
  of `./lakehouse/spark` at `/opt/lakehouse:ro` via `HOST_REPO_DIR` env since the socket runs on the host).
- `ingest_health` (`*/15 * * * *`): connector RUNNING (Connect REST), bronze freshness (latest
  `ingest_ts` of `bronze/olist/orders` within `BRONZE_FRESHNESS_HOURS` default 24) via `deltalake`.
- `silver_hourly` (`@hourly`): `spark_task(silver)` → `quality run` (PythonOperator or `BashOperator`;
  exit 1 fails) → outlet `SILVER`.
- `gold_daily` (`schedule=[SILVER]`): Cosmos `DbtTaskGroup` over `/opt/airflow/analytics`, profile
  `retail`/`local`, `dbt_executable_path=/opt/dbt-venv/bin/dbt`, install deps; outlet `GOLD`.
- `lakehouse_maintenance` (`@weekly`): `spark_task(maintain)`.
- tests (`uv run --project orchestration pytest orchestration/tests`): DagBag loads with zero import
  errors; the four DAG ids exist; every task has owner and retries ≥ 1; `gold_daily` schedule is the
  `SILVER` asset; `silver_hourly` outlets include `SILVER`; failure callback posts the JSON (mock HTTP).
- [ ] Verify on the stack: trigger `silver_hourly` once → success, `gold_daily` auto-triggers → success;
  paste task states (`airflow tasks states-for-dag-run`).
- [ ] Commit `feat(orchestration): dataset-aware airflow dags with dag integrity tests`.

---

### Task 6: Power BI semantic model (PBIP/TMDL) and page spec

**Owner:** `analytics-engineer`. Depends on T3.

- `bi/retail.pbip` + `bi/retail.SemanticModel/` (TMDL): a `GoldFolder` parameter (default
  `E:\Data Engineer and Analytics\retail-data-platform\data\gold`), one table per exported Parquet
  (Power Query `Parquet.Document(File.Contents(GoldFolder & "\<model>.parquet"))`), star relationships
  (facts → dims on surrogate keys, `dim_date` on date keys), DAX measures mirroring the dbt metrics
  exactly (Revenue, Orders, AOV, Conversion Rate, Late Delivery Rate) with a comment naming the dbt metric.
- `bi/README.md`: how to open (Power BI Desktop, enable PBIP preview if needed, set `GoldFolder`,
  refresh), and the page spec — Executive, Funnel, Customers (RFM, cohorts), Products/Sellers, Delivery
  SLA, Forecast vs Actual (placeholder until Phase 4), Data Quality — listing visuals and measures per
  page. The `.pbix`, report pages and screenshots are a **manual step for the human** (no agent can
  drive Power BI Desktop); say so at the top.
- [ ] Verify: TMDL files parse as text (no tooling available — reviewer checks syntax by reading);
  every measure maps to a dbt metric.
- [ ] Commit `feat(bi): power bi pbip semantic model over gold parquet and page spec`.

---

### Task 7: CI, ADR-0005, runbook, README

**Owner:** `platform-engineer`. Depends on T3, T5.

- CI `lint-test`: `make dbt-parse` (both targets), `uv run sqlfluff lint analytics/models`,
  `uv run --project orchestration pytest orchestration/tests`.
- CI `ingest`: after `test-ingest` (silver populated on the sample), `make gold` and `uv run dbt test`
  (unit + data tests) — sample-sized, so marts are tiny.
- `docs/adr/0005-gold-and-orchestration.md` (≤ 40 lines): DuckDB + `delta_scan` (or delta plugin) over
  silver; external Parquet for Power BI and Feast; Snowflake target parity via dispatch; MetricFlow as the
  single metric definition; Airflow standalone with LocalExecutor (one container; production would split
  scheduler/api/dag-processor); Spark via DockerOperator over the host socket; Airflow as a separate uv
  project; PBIP/TMDL instead of a binary `.pbix` in git.
- `docs/runbooks/analytics.md`: `make gold`, opening Power BI, Airflow UI/login, triggering DAGs, alerts.
- README: Phase 3 status, "Run it" analytics section.
- [ ] Commit `ci: dbt parse/lint, gold build, dag tests; docs: ADR-0005, analytics runbook`.

Waves: **1** = T1 ∥ T4 → **2** = T2 ∥ T5 → **3** = T3 → **4** = T6 ∥ T7.
