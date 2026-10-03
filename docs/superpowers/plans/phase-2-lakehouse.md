# Phase 2 — Lakehouse (Silver) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Spark batch job turns bronze into cleaned, typed, conformed silver Delta tables (current
state for facts, SCD2 for products/customers/sellers), every rejected row lands in
`silver/_rejects/` with a `rule_id`, per-rule reject metrics are queryable, and Great Expectations
gates fail loudly on critical problems.

**Architecture:** `silver/job.py` runs one `availableNow` Delta streaming read per bronze table
(incremental, checkpointed, exactly-once), and in `foreachBatch`: flatten the Debezium envelope →
apply the table's rule chain (each rule returns kept + rejected) → write rejects and rule metrics →
`MERGE` into silver (latest-LSN-wins for current-state tables, insert-new-versions + recompute for
SCD2, insert-if-absent on `event_id` for events). `quality run` (host-side, `deltalake` + pandas + GX)
validates silver, writes `_dq_results`, exits 1 on any critical failure. `maintain` runs
`OPTIMIZE … ZORDER BY` + `VACUUM` on bronze and silver.

**Tech Stack:** PySpark 4.0.4 + Delta 4.0.1 (existing spark image, no new jars), Great Expectations
1.x, `deltalake`, pandas/pyarrow, uv, Docker Compose profile `ingest`.

**Spec:** `docs/superpowers/specs/retail-data-platform-design.md` §6 (and §5 for bronze shape).
Phase 1 is merged on `main`; bronze shape: CDC rows `op, ts_ms, before, after, source` + `kafka_topic,
kafka_partition, kafka_offset, kafka_timestamp, schema_id, ingest_ts, ingest_date` at
`bronze/olist/<table>`; events at `bronze/events/<type>` with the Avro fields + the same metadata.

## Global Constraints

- Python 3.12 uv workspace, explicit `members` list in root `pyproject.toml`; `ruff`, `mypy --strict`
  clean (`make lint` covers `ingestion lakehouse tests`).
- Cleaning rules are pure functions on Spark DataFrames, each with a `rule_id` and a `spark`-marked
  pytest on a tiny in-memory DataFrame. Test module names unique across the repo (mypy).
- Nothing dropped silently: every row that does not reach silver is in `_rejects` with `rule_id`
  and `reason`, except superseded CDC versions of current-state tables (that is history, kept in bronze).
- Every silver write is idempotent: re-running the job on the same bronze data changes nothing.
- Spark session time zone `UTC`. Olist timestamps are São Paulo wall clock: `<x>_local`
  (`timestamp_ntz`) keeps the original, `<x>_utc` = `to_utc_timestamp(local, "America/Sao_Paulo")`.
- Silver root `s3a://${LAKEHOUSE_BUCKET}/silver`; checkpoints `s3a://${LAKEHOUSE_BUCKET}/_checkpoints/silver/<domain>_<table>`.
- No new Docker images or jars. New Python deps only in the new `lakehouse/quality` package.
- Conventional commits. Parallel tasks run in separate git worktrees and touch only their own files.
- Scheduling (silver hourly, `maintain` weekly, quality after each silver load) and the Power BI quality
  page belong to Phase 3 (Airflow, BI); this phase delivers runnable `make` targets with correct exit codes.

## Review Focus

1. **Same bronze batch processed twice** (job crash after MERGE, before checkpoint commit) must not
   duplicate SCD2 versions, events or rejects. → T1 (SCD2 rerun test), T3 (rerun job test).
2. **No-op updates** (only `updated_at` changed — Phase 1 smoke ran 50k of them) must not create SCD2
   versions; they still advance `_source_lsn` on current-state tables. → T1.
3. **Delete (`op = d`)**: `after` is null; values come from `before`, `_is_deleted = true`, row kept.
   → T1.
4. **DST**: 2017-01-15 10:00 São Paulo (UTC-2, summer time) → 12:00 UTC; 2017-07-01 10:00 (UTC-3)
   → 13:00 UTC. → T2.
5. **Bronze schema evolution** (events v2 adds `utm_campaign` while a silver run is mid-backlog):
   the Delta streaming source fails with a schema-change error; the job must restart that table's
   stream (max 3 attempts) and continue. → T3.

## Silver contract (shared by all tasks)

Metadata columns on every CDC-derived silver table: `_source_lsn` bigint, `_source_ts` timestamp
(`source.ts_ms`), `_is_deleted` boolean, `_silver_loaded_at` timestamp, `_run_id` string.
SCD2 tables add `valid_from` timestamp, `valid_to` timestamp (null = open), `is_current` boolean.
Column `updated_at` is dropped everywhere.

| Silver path (under `silver/`) | Mode | Key | Columns (besides metadata) |
|---|---|---|---|
| `catalog/categories` | current | `product_category_name` | `product_category_name_english` |
| `catalog/products` | scd2 | `product_id` | `product_category_name`, `product_category_name_english` (fallback `unknown`), `product_name_length`, `product_description_length`, `product_photos_qty`, `product_weight_g`, `product_length_cm`, `product_height_cm`, `product_width_cm` |
| `party/customers` | scd2 | `customer_id` | `customer_unique_id`, `customer_zip_code_prefix`, `customer_city`, `customer_state` |
| `party/sellers` | scd2 | `seller_id` | `seller_zip_code_prefix`, `seller_city`, `seller_state` |
| `geo/geolocation_points` | current | `geolocation_pk` | `zip_code_prefix`, `lat`, `lng`, `city`, `state` |
| `geo/zip_centroids` | overwrite (derived) | `zip_code_prefix` | `lat`, `lng` (means), `n_points`, `state` (most frequent, ties → min) |
| `sales/orders` | current | `order_id` | `customer_id`, `order_status`, `order_purchase_ts_{local,utc}`, `order_approved_ts_*`, `order_delivered_carrier_ts_*`, `order_delivered_customer_ts_*`, `order_estimated_delivery_ts_*`, `flag_approved_before_purchase`, `flag_carrier_before_approved`, `flag_delivered_before_carrier`, `flag_delivered_before_purchase` |
| `sales/order_items` | current | `order_id, order_item_id` | `product_id`, `seller_id`, `shipping_limit_ts_{local,utc}`, `price` decimal(12,2), `freight_value` decimal(12,2) |
| `sales/order_payments` | current | `order_id, payment_sequential` | `payment_type`, `payment_installments`, `payment_value` |
| `sales/order_reviews` | current | `review_pk` | `review_id`, `order_id`, `review_score`, `review_comment_title`, `review_comment_message`, `review_creation_ts_*`, `review_answer_ts_*` |
| `events/clickstream` | insert-if-absent on `event_id`, partitioned `event_date` | `event_id` | `event_type`, `session_id`, `customer_id`, `device`, `referrer`, `event_ts_local` (ntz), `event_ts_utc`, `product_id`, `search_query`, `quantity`, `order_id`, `utm_campaign`, `event_date`, `_bronze_ingest_ts`, `_silver_loaded_at`, `_run_id` |
| `_rejects/<domain>/<table>` | append | — | `rule_id`, `reason`, `record_json` (the rejected row as JSON), `_run_id`, `_rejected_at` |
| `_rule_metrics` | append | — | `run_id`, `table` (`"sales/orders"`), `rule_id`, `rows_in`, `rows_rejected`, `pct_rejected` double, `run_ts` |
| `_dq_results` | append (written by `quality`) | — | `run_id`, `table`, `expectation`, `severity` (`critical`/`warning`), `success`, `observed_value` string, `details` string, `checked_at` |

Rule ids (exact): `cdc_exact_duplicate`, `orders_null_purchase_ts`, `geo_out_of_bbox`,
`event_cast_failed`, `event_negative_quantity`, `event_duplicate`, `session_over_24h`.
Non-rejecting transforms also carry ids for documentation and metrics (`rows_rejected = 0`):
`cdc_collapse`, `ts_localise`, `orders_timeline_flags`, `category_translate`.

Olist's "duplicated order items" are quantity units (one row per unit, distinct `order_item_id`),
not defects; silver keeps one row per `(order_id, order_item_id)`. True duplicates are redelivered
CDC records with the same key and LSN → `cdc_exact_duplicate`. (ADR-0004.)

## File Structure

```
lakehouse/spark/src/lakehouse_spark/silver/
  __init__.py
  rules.py          Rule, apply_rules, RuleMetric                                 (T2)
  cdc.py            flatten_cdc, exact_duplicates, collapse_latest, merge_current  (T1)
  scd2.py           scd2_new_versions, scd2_recompute, merge_scd2                  (T1)
  domain/__init__.py, sales.py, catalog.py, geo.py, events.py                      (T2)
  tables.py         TableSpec registry (bronze source, silver path, mode, key, rules) (T3)
  job.py            run_table, main                                               (T3)
  maintain.py       optimize_and_vacuum, main                                     (T3)
lakehouse/spark/src/lakehouse_spark/cli.py       status also lists silver + latest rule metrics (T3)
lakehouse/spark/tests/test_silver_cdc.py, test_silver_scd2.py                     (T1)
lakehouse/spark/tests/test_silver_rules.py, test_silver_sales.py, test_silver_catalog.py,
  test_silver_geo.py, test_silver_events.py                                       (T2)
lakehouse/spark/tests/test_silver_job.py, test_silver_maintain.py                 (T3)
lakehouse/quality/pyproject.toml   package `lakehouse-quality`, script `quality`   (T4)
lakehouse/quality/src/lakehouse_quality/{__init__,io,suites,run}.py               (T4)
lakehouse/quality/tests/test_quality_*.py                                          (T4)
tests/integration/test_ingest.py   + silver/quality stage                       (T5)
Makefile (silver, quality, maintain targets), .github/workflows/ci.yml, docs/adr/0004-silver-design.md,
  docs/runbooks/lakehouse.md, README.md                                             (T3 Makefile; T6 rest)
```

Waves: **1** = T1 ∥ T2 ∥ T4 (separate worktrees) → merge → **2** = T3 → **3** = T5 ∥ T6.

---

### Task 1: CDC flattening, current-state MERGE, SCD2

**Owner:** `data-engineer`. Worktree. Touches only `silver/cdc.py`, `silver/scd2.py`,
`silver/__init__.py`, `tests/test_silver_cdc.py`, `tests/test_silver_scd2.py`.

**Interfaces (produced):**
```python
# cdc.py
META = ("_source_lsn", "_source_ts", "_is_deleted")
def flatten_cdc(bronze: DataFrame) -> DataFrame
    # one row per bronze record: coalesce(after, before).* minus updated_at, plus
    # _op (op), _source_lsn (source.lsn), _source_ts (timestamp of source.ts_ms), _is_deleted (op == 'd'),
    # _kafka_offset (kafka_offset)
def exact_duplicates(df: DataFrame, key: Sequence[str]) -> tuple[DataFrame, DataFrame]
    # kept, rejected: rows with the same (*key, _source_lsn) after the first (lowest _kafka_offset); rejected gets reason
    # "duplicate delivery of key+lsn"
def collapse_latest(df: DataFrame, key: Sequence[str]) -> DataFrame   # one row per key: max (_source_lsn, _kafka_offset)
def merge_current(spark: SparkSession, latest: DataFrame, path: str, key: Sequence[str]) -> None
    # creates the table on first write; MERGE ON key: WHEN MATCHED AND s._source_lsn >= t._source_lsn UPDATE *,
    # WHEN NOT MATCHED INSERT *. Drops _op, _kafka_offset before writing.
# scd2.py
BEGINNING_OF_TIME = datetime(1900, 1, 1)
def scd2_new_versions(existing: DataFrame | None, changes: DataFrame, key: Sequence[str], tracked: Sequence[str]) -> DataFrame
    # changes = flattened rows; returns the versions to INSERT: drop (key, _source_lsn) already in existing; order
    # existing ∪ changes per key by _source_lsn and drop a change whose hash(tracked + _is_deleted) equals its
    # predecessor's; valid_from = _source_ts, except a key's first-ever version with _op == 'r' → BEGINNING_OF_TIME
def scd2_recompute(versions: DataFrame, key: Sequence[str]) -> DataFrame
    # valid_to = lead(valid_from) per key ordered by _source_lsn; is_current = valid_to IS NULL
def merge_scd2(spark: SparkSession, changes: DataFrame, path: str, key: Sequence[str], tracked: Sequence[str]) -> int
    # (1) MERGE insert-if-absent on (key, _source_lsn) the new versions; (2) recompute valid_to/is_current for the
    # touched keys and MERGE UPDATE rows whose valid_to/is_current changed. Returns versions inserted. Both steps
    # idempotent, so a rerun of the same batch is a no-op.
```

- [ ] **Step 1: Failing tests** (`@pytest.mark.spark`; build bronze-shaped rows with `before/after`
  structs in-test; use the existing `spark` and `root` fixtures):
  `test_flatten_uses_before_for_deletes` (op d → values from before, `_is_deleted` true, updated_at gone),
  `test_exact_duplicates_rejects_same_key_and_lsn_only` (same key different lsn kept),
  `test_collapse_latest_picks_max_lsn_then_offset`,
  `test_merge_current_latest_wins_regardless_of_batch_order` (apply newer batch then older → newer stays),
  `test_merge_current_rerun_is_noop`,
  `test_scd2_noop_update_creates_no_version` (Review Focus 2: only updated_at differs),
  `test_scd2_change_closes_previous_and_opens_new` (valid_to == next valid_from, one is_current),
  `test_scd2_snapshot_version_starts_at_beginning_of_time`,
  `test_scd2_multiple_versions_in_one_batch` (three changes, two real → 3 rows total incl. initial, contiguous),
  `test_scd2_rerun_same_batch_is_noop` (Review Focus 1), `test_scd2_delete_is_a_version` (Review Focus 3).
- [ ] **Step 2:** `make test-spark` → FAIL (import). Paste tail.
- [ ] **Step 3:** Implement. **Step 4:** `make test-spark` → PASS; `make lint` PASS.
- [ ] **Step 5:** Commit `feat(lakehouse): cdc flattening, current-state merge and scd2`.

---

### Task 2: Rule framework and domain cleaning rules

**Owner:** `data-engineer`. Worktree. Touches only `silver/rules.py`, `silver/domain/*`, and the five
`tests/test_silver_{rules,sales,catalog,geo,events}.py`.

**Interfaces (produced):**
```python
# rules.py
@dataclass(frozen=True)
class Rule:
    rule_id: str
    description: str
    fn: Callable[[DataFrame], tuple[DataFrame, DataFrame]]   # (kept, rejected); rejected = input columns + "reason"
@dataclass(frozen=True)
class RuleMetric:
    rule_id: str; rows_in: int; rows_rejected: int
def apply_rules(df: DataFrame, rules: Sequence[Rule]) -> tuple[DataFrame, DataFrame, list[RuleMetric]]
    # rejected unified to columns rule_id, reason, record_json (to_json(struct(*input columns)))
def transform(rule_id: str, description: str, fn: Callable[[DataFrame], DataFrame]) -> Rule   # non-rejecting
# domain/sales.py
LOCAL_TZ = "America/Sao_Paulo"
def localise(df: DataFrame, columns: Mapping[str, str]) -> DataFrame   # {source_col: out_prefix} → <prefix>_local, <prefix>_utc; drops source
ORDERS_RULES, ORDER_ITEMS_RULES, PAYMENTS_RULES, REVIEWS_RULES: tuple[Rule, ...]
# domain/catalog.py
PRODUCT_RENAMES = {"product_name_lenght": "product_name_length", "product_description_lenght": "product_description_length"}
def products_rules(categories: DataFrame | None) -> tuple[Rule, ...]   # renames + category_translate (fallback "unknown")
CATEGORIES_RULES: tuple[Rule, ...]
# domain/geo.py
BRAZIL_BBOX = {"lat_min": -33.75, "lat_max": 5.27, "lng_min": -73.99, "lng_max": -34.79}
GEO_RULES: tuple[Rule, ...]            # renames geolocation_* → zip_code_prefix/lat/lng/city/state; geo_out_of_bbox
def zip_centroids(points: DataFrame) -> DataFrame
# domain/events.py
def event_rules(existing: DataFrame | None) -> tuple[Rule, ...]
    # event_cast_failed (event_ts not "yyyy-MM-dd'T'HH:mm:ss"), ts_localise (event_ts → event_ts_local/_utc, event_date),
    # event_negative_quantity (quantity < 0), event_duplicate (event_id seen earlier in batch by _bronze_ingest_ts then
    # kafka_offset, or present in existing.event_id), session_over_24h (event_ts_local > session start + 24 h, session
    # start = min over batch ∪ existing for that session_id)
```
Orders rules in order: `orders_null_purchase_ts` (reject), `ts_localise` (five columns → prefixes
`order_purchase_ts`, `order_approved_ts`, `order_delivered_carrier_ts`, `order_delivered_customer_ts`,
`order_estimated_delivery_ts`), `orders_timeline_flags` (four flags, true only when both sides
non-null and inverted). Items/reviews: `ts_localise` (`shipping_limit_date`→`shipping_limit_ts`;
`review_creation_date`→`review_creation_ts`, `review_answer_timestamp`→`review_answer_ts`).

- [ ] **Step 1: Failing tests**: `test_apply_rules_accounting` (rows_in − rejected == kept per rule;
  record_json round-trips), `test_localise_dst_summer` (2017-01-15 10:00 → 12:00 UTC) and
  `test_localise_winter` (2017-07-01 10:00 → 13:00 UTC) (Review Focus 4), `test_orders_flags_only_when_both_present`,
  `test_orders_null_purchase_rejected`, `test_category_translate_fallback_unknown` (null category and
  untranslated category), `test_product_renames`, `test_geo_bbox_rejects_outside_brazil` (incl. boundary
  points kept), `test_zip_centroids_mean_and_mode_state`, `test_event_cast_failed`, `test_event_negative_quantity`,
  `test_event_duplicate_within_batch_and_vs_existing`, `test_session_over_24h_uses_existing_start`.
- [ ] **Step 2:** `make test-spark` → FAIL. **Step 3:** implement. **Step 4:** PASS; `make lint`.
- [ ] **Step 5:** Commit `feat(lakehouse): silver rule framework and domain cleaning rules`.

---

### Task 3: Silver job, maintenance, status, Make targets

**Owner:** `data-engineer`. Depends on T1, T2. Exception: `Makefile` targets `silver`, `quality`
(calls T4's CLI), `maintain`.

**Interfaces:**
```python
# tables.py
@dataclass(frozen=True)
class TableSpec:
    name: str                    # "sales/orders"
    bronze: tuple[str, ...]      # bronze relative paths, e.g. ("olist/orders",) or the five events/<type>
    mode: Literal["current", "scd2", "events"]
    key: tuple[str, ...]
    tracked: tuple[str, ...] = ()            # scd2 only
    rules: Callable[[SparkSession, str], tuple[Rule, ...]]   # (spark, silver_root) → rules (lets products read categories, events read existing)
TABLES: tuple[TableSpec, ...]   # run order: catalog/categories, catalog/products, party/customers, party/sellers,
                                # geo/geolocation_points, sales/orders, sales/order_items, sales/order_payments,
                                # sales/order_reviews, events/clickstream
# job.py
def run_table(spark: SparkSession, spec: TableSpec, root: str, run_id: str) -> None
    # per bronze source: readStream delta (bronze path) → trigger(availableNow=True) → foreachBatch:
    #   flatten (CDC) / select (events) → apply exact_duplicates + spec.rules → write rejects, append _rule_metrics →
    #   merge_current | merge_scd2 | events insert-if-absent; checkpoint _checkpoints/silver/<domain>_<table>[_<type>].
    # On a Delta streaming schema-change error restart that stream, max 3 attempts (Review Focus 5).
    # After geo/geolocation_points, overwrite geo/zip_centroids from the current points if the batch was non-empty.
def main(argv: list[str] | None = None) -> int   # --tables a,b (default all); run_id = UTC timestamp + 6 hex
# maintain.py
ZORDER = {"sales/orders": ("customer_id",), "sales/order_items": ("product_id", "seller_id"),
          "events/clickstream": ("session_id",)}
def optimize_and_vacuum(spark: SparkSession, root: str, retain_hours: int = 168) -> dict[str, int]  # path → files after
```
Make: `silver` = `docker compose --profile ingest run --rm --no-deps spark spark-submit /opt/lakehouse/src/lakehouse_spark/silver/job.py $(SILVER_ARGS)`;
`maintain` likewise with `maintain.py`; `quality` = `uv run --package lakehouse-quality quality run`.
`bronze status` (cli.py) also lists `silver/*` tables (current rows for SCD2: `is_current`), each
`silver/_rejects/*`, and for the latest run in `_rule_metrics` one line per rule with rejects:
`rule geo_out_of_bbox rejected 27 rows (0.003 %) in geo/geolocation_points`.

- [ ] **Step 1: Failing tests** (`spark`): `test_run_table_current_end_to_end` (write a tiny bronze Delta
  table to `root`, run, assert silver rows, rejects, metrics), `test_run_table_rerun_noop` (second run:
  no new rows/rejects/metrics with rows_in > 0) (Review Focus 1), `test_run_table_scd2`,
  `test_run_table_events_dedupes_across_runs`, `test_schema_change_restart` (append a v2-shaped batch to
  the bronze events table between two runs and once mid-run if feasible; job completes and the new
  column appears), `test_optimize_reduces_files`; host test in `test_cli.py` for the silver/metrics lines.
- [ ] **Step 2:** FAIL. **Step 3:** implement. **Step 4:** `make test-spark`, `uv run pytest lakehouse -m "not spark"`, `make lint` PASS.
- [ ] **Step 5: Smoke on the dev stack** (full Olist bronze from Phase 1): `make silver`; report wall time,
  `make status`, silver row counts vs `replayer status`, rule metrics lines, `docker stats` peak for the
  run container; run `make silver` again → no changes; `make maintain` → file counts before/after.
- [ ] **Step 6:** Commit `feat(lakehouse): silver job, maintenance and status`.

---

### Task 4: Great Expectations quality gates

**Owner:** `data-engineer`. Worktree. New package `lakehouse/quality` (append `"lakehouse/quality"` to
root members; `uv lock`). No Spark: reads Delta with `deltalake` → pandas.

**Interfaces:**
```python
# io.py
def storage_options() -> dict[str, str]     # from MINIO_* env + MINIO_ENDPOINT (default http://127.0.0.1:9000); {} for file:// roots
def read_silver(root: str, table: str, current_only: bool = True) -> pd.DataFrame   # SCD2: is_current rows; all: not _is_deleted
def bronze_distinct_keys(root: str, bronze_table: str, key: Sequence[str]) -> int  # distinct key over coalesce(after, before)
# suites.py
@dataclass(frozen=True)
class Check:
    table: str; expectation: str; severity: Literal["critical", "warning"]; success: bool; observed: str; details: str
def run_checks(root: str, now: datetime) -> list[Check]
# run.py
def main(argv: list[str] | None = None) -> int   # `quality run [--root s3://lakehouse]`: run_checks → append _dq_results
                                                  # → print one line per check → exit 1 if any critical failed
```
Checks (use GX 1.x expectations on pandas batches where one exists; plain pandas for the cross-table ones):
- critical per table: expected columns present (silver contract), key not null and unique (current rows);
  row-count reconciliation for CDC tables: `silver distinct key ≤ bronze distinct key` and
  `≥ bronze distinct key − rejected distinct keys − bronze_distinct_key × DQ_ROWCOUNT_TOLERANCE (0.001)`;
  referential integrity order_items→orders/products/sellers, payments→orders, reviews→orders,
  orders→customers (current dims); `price > 0`.
- warning: `review_score` in 1..5; freight: rows of the latest 30 days of `order_purchase_ts_utc` with
  `freight_value ≤ p99` of the earlier rows, `mostly=0.95`; events `product_id` ∈ products `mostly=0.99`;
  freshness `max(_silver_loaded_at) ≥ now − DQ_FRESHNESS_HOURS (2)` per table.
  Env defaults documented in `.env.example` (exception granted for `DQ_*` lines).

- [ ] **Step 1: Failing tests** (host, no marker): build tiny silver/bronze Delta tables under `tmp_path`
  with `deltalake.write_deltalake`; `test_clean_tables_pass_all`, `test_duplicate_key_is_critical`,
  `test_orphan_order_item_is_critical`, `test_rowcount_gap_is_critical`, `test_stale_table_is_warning`,
  `test_main_exit_codes_and_dq_results_appended` (exit 0 with only warnings, 1 with a critical; rows in `_dq_results`).
- [ ] **Step 2:** FAIL. **Step 3:** implement. **Step 4:** `uv run pytest lakehouse/quality`, `make lint` PASS.
- [ ] **Step 5:** Commit `feat(lakehouse): great expectations quality gates`.

---

### Task 5: Silver stage in the end-to-end test

**Owner:** `data-engineer`. Depends on T3, T4. Touches `tests/integration/test_ingest.py` only.

Append a final test function (runs after the ingest assertions in the same module, same stack state):
1. `make silver` (subprocess) → exit 0; then assert on the sample: `sales/orders` 200 rows and every
   `order_status` equals Postgres; `order_items` 229, `order_payments` 211, `order_reviews` 200;
   current `party/customers` == distinct customers in Postgres; `catalog/products` current 198,
   `party/sellers` 162; `geo/geolocation_points` + geo rejects == 1000; `events/clickstream` + event rejects
   == bronze events rows; SCD2 invariants (one `is_current` per key, `valid_to` null ⇔ current, no overlaps).
2. `UPDATE olist.customers SET updated_at = now()` on one row and `SET customer_city = 'x'` on another →
   wait for bronze (poll `bronze status`) → `make silver` → first customer still 1 version, second 2.
3. `make silver` again → no new rows anywhere (counts equal), latest `_rule_metrics` run has `rows_in == 0`.
4. `make quality` → exit 0; `_dq_results` has rows for the run; no critical failures.

- [ ] **Step 1:** write it; **Step 2:** `ALLOW_RESEED=1 make test-ingest` once → PASS (report wall time).
  Max 3 full runs; debug from logs between runs.
- [ ] **Step 3:** Commit `test(lakehouse): silver and quality stage in ingest e2e`.

---

### Task 6: CI, ADR-0004, runbook, README

**Owner:** `platform-engineer`. Depends on T3, T4 (parallel with T5; no docker).

- [ ] CI: `ingest` job already runs `test-ingest`, which now includes silver; add `uv run pytest lakehouse/quality`
  to `lint-test` via `make test` if not already collected (check `testpaths`).
- [ ] `docs/adr/0004-silver-design.md` (≤ 40 lines): Delta streaming source with `availableNow` instead of
  Change Data Feed (bronze is append-only, so CDF adds nothing; CDF reserved for tables with updates);
  latest-LSN MERGE; SCD2 in processing time from CDC with `1900-01-01` for snapshot rows and no-op-update
  suppression; Olist item "duplicates" are quantity; GE on pandas via `deltalake` (no Spark in gates);
  partitioning only `events/clickstream` by `event_date` (orders ~100k rows: partitions would be tiny),
  Z-order keys; rule ids list.
- [ ] `docs/runbooks/lakehouse.md`: `make silver`, `make quality`, `make maintain`, reading rule metrics,
  inspecting rejects with `deltalake`, resetting silver (delete `silver/` + `_checkpoints/silver/`).
- [ ] README: Phase 2 status line, "Run it" lines for silver/quality.
- [ ] Commit `docs: ADR-0004 silver design, lakehouse runbook; ci: quality tests`.
