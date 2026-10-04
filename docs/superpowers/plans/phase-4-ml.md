# Phase 4 — Batch ML and MLOps Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Two production-shaped models: late-delivery risk (classification at approval, served online
and scored in batch) and 28-day demand forecast per category × state (batch). Features come from
gold via Feast (point-in-time offline, Redis online), training is config-driven with MLflow tracking
and champion/challenger promotion, a FastAPI service serves the champion with Prometheus metrics, and
Evidently monitoring can trigger retraining, all orchestrated by Airflow.

**Architecture:** dbt builds ML-ready gold tables (training rows with labels, daily seller feature
snapshots, daily demand series) as Parquet. A separate uv project `ml/` holds one Python package
`retail_ml` with a CLI (`retail-ml train|score|forecast|monitor|materialize`), a Feast repo, and the
FastAPI app; one `ml` image runs the API and every Airflow ML task (DockerOperator, as with Spark).
MLflow server (Postgres metadata + MinIO artefacts) holds runs and the registry; models carry aliases
`champion`/`challenger`. Prometheus scrapes the API; Grafana shows a committed dashboard.

**Tech Stack:** LightGBM, scikit-learn, MLflow 3.x, Feast (Redis online, file offline), FastAPI +
uvicorn + prometheus-client, Evidently, SHAP, Locust, Redis, Prometheus, Grafana, dbt (feature models).

**Spec:** `docs/superpowers/specs/retail-data-platform-design.md` §8 (§9 only for "same feature
definitions reused by real-time"). Phase 3 gold: star schema + marts as Parquet in `data/gold/`.

## Global Constraints

- `ml/` is a **separate uv project** (own `pyproject.toml` + `uv.lock`, not a workspace member): Feast,
  MLflow and Evidently pin pandas/pyarrow ranges that conflict with the workspace (GX → pandas 3).
  Commands: `uv run --project ml …`. Python 3.12. Dependencies ≥ 7 days old, `exclude-newer` absolute
  like the root. All runtime deps declared once in T3's `ml/pyproject.toml` (parallel tasks don't touch it
  except to add dev tools, coordinated by the lead).
- No training/serving skew: order-level features are computed by one pure function
  `retail_ml.features.order_features(df)` used by training, batch scoring and the API; seller features
  come only from Feast (offline for training/scoring, online for the API).
- Point-in-time correctness: training rows are `as of order_approved_ts_utc`; seller features are daily
  snapshots built only from orders **delivered before** the snapshot date.
- Time-based splits (late delivery): train `approved < 2018-03-01`, validation `[2018-03-01, 2018-06-01)`,
  test `[2018-06-01, 2018-09-01)`; only delivered orders have labels. Forecast: last 28 days of data =
  test; 3-fold rolling-origin backtest (28-day folds) before it.
- Every baseline is logged before its model: late delivery — constant prior and logistic regression;
  forecast — seasonal naive (same weekday, 7 days earlier, repeated over the horizon).
- Every MLflow run logs params, metrics, artefacts, `data_sha256` of the training Parquet, `git_sha`,
  model signature and input example. Registered models `late_delivery`, `demand_forecast`.
- Promotion: a challenger becomes `champion` only if it beats the champion's primary metric on the
  same test window (late delivery PR-AUC higher; forecast WAPE lower) **and** passes model tests
  (calibration: Brier ≤ logistic baseline; no NaN predictions; prediction range [0,1]). `PROMOTION_MODE`
  `auto` (default) or `manual` (sets `challenger` only and prints the approve command).
- Images pinned `tag@sha256`; memory limits; ports on 127.0.0.1. Config via env with `.env.example`.
- Conventional commits; parallel tasks in separate worktrees with disjoint files.

## Review Focus

1. **Leakage** through seller features computed from orders not yet delivered at approval time, or a
   snapshot dated the same day as the order. Test with a hand-built seller history. → T1, T3.
2. **Multi-seller / multi-item orders** (≈1 % have >1 seller): primary seller = seller of the
   highest-price item, ties → smallest `seller_id`; order totals summed over items. → T1.
3. **Unknown categorical values at serving time** (new category, payment type, unseen seller with no
   Feast row): the API returns a probability (defaults for missing features), never 500. → T3, T5.
4. **Sparse demand series** (many category × state pairs with mostly zero days): only series with ≥ 1
   order on ≥ 50 % of days in the 180 days before the cutoff are modelled; others reported as excluded.
   → T1, T4.
5. **Registry empty** (fresh stack, no champion yet): the API starts, `/health` reports
   `model_loaded=false`, `/predict` returns 503 with a clear message; first training run with no champion
   promotes directly. → T3, T5.

## Data contracts (gold → ML)

`data/gold/ml/late_delivery_training.parquet` (one row per order with `order_approved_ts_utc` not null):
`order_id, customer_id, customer_unique_id, seller_id (primary), order_status,
order_purchase_ts_utc, order_approved_ts_utc, order_estimated_delivery_ts_utc,
order_delivered_customer_ts_utc, is_late (bool, null if not delivered), n_items, n_sellers,
total_price, total_freight, product_category (primary item, English), payment_type
(payment_sequential = 1), payment_installments, customer_state, customer_lat, customer_lng,
seller_state, seller_lat, seller_lng`.

`data/gold/ml/seller_features_daily.parquet`: `seller_id, feature_ts (date 00:00 UTC),
seller_orders_90d, seller_late_rate_90d, seller_avg_delivery_days_90d, created_ts` — over orders whose
`order_delivered_customer_ts_utc < feature_ts` and `≥ feature_ts − 90 days`; one row per seller per day
from the seller's first delivery to the dataset end.

`data/gold/ml/demand_daily.parquet`: `date, product_category, customer_state, orders (distinct revenue
orders), is_modelled (bool per Review Focus 4)` — zero-filled per series from its first order date.

Order-level features (`order_features`): `freight_ratio = total_freight / nullif(total_price, 0)`,
`distance_km` (haversine customer↔seller), `n_items`, `n_sellers`, `estimated_days =
(estimated − approved) in days`, `approve_hour`, `approve_dow`, `approve_month`, `payment_type`,
`payment_installments`, `product_category`, `customer_state`, `seller_state`, `same_state`.
Seller features (Feast view `seller_stats`): the three `seller_*_90d` columns.

## File Structure

```
analytics/models/ml/ml_late_delivery_training.sql, ml_seller_features_daily.sql, ml_demand_daily.sql, _ml.yml   (T1)
ml/pyproject.toml, uv.lock, Dockerfile, README.md                                                            (T3; Dockerfile T2)
ml/src/retail_ml/__init__.py, cli.py, config.py, data.py (gold readers, data hash)                        (T3)
ml/src/retail_ml/features.py (order_features, haversine)                                                    (T3)
ml/src/retail_ml/late_delivery/{train,evaluate,promote,score}.py                                            (T3; score.py T4)
ml/src/retail_ml/forecast/{features,train,predict}.py                                                       (T4)
ml/src/retail_ml/serving/{app,model,metrics}.py, ml/locustfile.py                                           (T5)
ml/src/retail_ml/monitoring/{drift,performance}.py                                                          (T6)
ml/feature_repo/{feature_store.yaml,features.py}                                                            (T3)
ml/configs/{late_delivery,demand_forecast}.yaml                                                             (T3, T4)
ml/tests/test_*.py                                                                                           (owner of each module)
docker-compose.yml (profiles ml, observability), observability/{prometheus,grafana}/…                        (T2)
orchestration/dags/{ml_common,feast_materialize,train_late_delivery,train_demand_forecast,
  score_late_delivery,forecast_demand,monitor_late_delivery}.py, orchestration/tests/test_ml_dags.py       (T7)
ds/notebooks/{01_late_delivery_eda,02_demand_eda}.ipynb, ds/reports/iteration_log.md,
  ds/model_cards/{late_delivery,demand_forecast}.md                                                          (T8)
.github/workflows/ci.yml, docs/adr/0006-ml-platform.md, docs/runbooks/ml.md, README.md                      (T9)
```

Waves: **1** = T1 ∥ T2 ∥ T3 → **2** = T4 ∥ T5 ∥ T6 → **3** = T7 ∥ T8 → **4** = T9.

---

### Task 1: ML feature and label models in dbt

**Owner:** `analytics-engineer`. Files under `analytics/models/ml/` only (+ `dbt_project.yml` config
block for `ml` models: external Parquet at `{{ GOLD_DIR }}/ml/<name without ml_ prefix>.parquet`).
- Build the three contracts above from the Phase 3 star schema / staging.
- Tests: keys unique/not null; `is_late` null iff not delivered; singular tests for Review Focus 1
  (a seller's feature row on day D counts only deliveries before D — unit test with fixtures), Review
  Focus 2 (primary seller tie-break, unit test), Review Focus 4 (`is_modelled` rule, unit test).
- [ ] Unit tests first (red), implement, `dbt build --select ml` green on dev data; paste row counts and
  the share of `is_modelled` series. Commit `feat(analytics): ml training, seller feature and demand models`.

### Task 2: ML and observability infrastructure

**Owner:** `platform-engineer`. Files: `ml/Dockerfile`, `docker-compose.yml`, `observability/…`,
`Makefile`, `.env.example`.
- `ml/Dockerfile`: `python:3.12-slim` pinned (or `ghcr.io/astral-sh/uv:python3.12-bookworm-slim`), `uv sync
  --project ml --locked --no-dev`, entrypoint `retail-ml`; non-root user. Builds once T3's lock exists — until
  then build against a stub `ml/pyproject.toml` only if T3 hasn't landed (coordinate via the lead).
- compose profile `ml`: `mlflow` (official image pinned; `mlflow server --backend-store-uri
  postgresql://…/mlflow --artifacts-destination s3://${LAKEHOUSE_BUCKET}/mlflow --serve-artifacts`,
  `MLFLOW_S3_ENDPOINT_URL=http://minio:9000`; an `mlflow-init` creates DB `mlflow` like airflow-init;
  port `127.0.0.1:${MLFLOW_PORT:-5000}`; limit 1 g), `redis` (pinned, `127.0.0.1:${REDIS_PORT:-6379}`,
  256 m), `serving` (ml image, `uvicorn retail_ml.serving.app:app --host 0.0.0.0 --port 8000`,
  `127.0.0.1:${SERVING_PORT:-8000}`, env MLFLOW_TRACKING_URI, FEAST/REDIS, mounts `./data:/data:ro`,
  `./ml/feature_repo:/feature_repo`, healthcheck `/health`, 1 g). postgres/minio/minio-init add `ml`.
- profile `observability`: `prometheus` (scrape `serving:8000/metrics` every 15 s; config committed),
  `grafana` (provisioned Prometheus datasource + dashboard JSON from `observability/grafana/dashboards/
  serving.json`: request rate, error rate, p50/p95 latency, prediction histogram, model version), anonymous
  viewer on 127.0.0.1 only.
- Make: `up PROFILE=ml`, `ml-build`, `ml ARGS=…` (= `docker compose --profile ml run --rm serving retail-ml $(ARGS)`).
- [ ] Verify: config quiet for both profiles; `make up PROFILE=ml` exit 0; MLflow UI 200 and an artefact
  round-trip to MinIO (`mlflow` client log_artifact from the container); Grafana shows the dashboard
  (empty data is fine). Commit `feat(platform): mlflow, redis, serving and observability profiles`.

### Task 3: retail_ml core — features, Feast repo, late-delivery training and promotion

**Owner:** `ml-engineer`. Worktree. Files: `ml/pyproject.toml`, `uv.lock`, `ml/src/retail_ml/{__init__,cli,
config,data,features}.py`, `late_delivery/{train,evaluate,promote}.py`, `ml/feature_repo/*`,
`ml/configs/late_delivery.yaml`, `ml/tests/test_{features,train_late,promote,feast_repo}.py`.
- `pyproject.toml` declares ALL runtime deps for T3-T6 (lightgbm, scikit-learn, mlflow, feast[redis],
  fastapi, uvicorn, prometheus-client, evidently, shap, pandas, pyarrow, boto3, pyyaml) + dev (pytest,
  httpx, locust, ruff, mypy); script `retail-ml = "retail_ml.cli:main"`.
- `features.order_features(df: pd.DataFrame) -> pd.DataFrame` (pure, columns per contract; categoricals
  as pandas `category` with an explicit `"__unknown__"` level), `haversine_km`.
- Feast repo: entity `seller` (`seller_id`), FileSource over `seller_features_daily.parquet`
  (`timestamp_field=feature_ts`, `created_timestamp_column=created_ts`), feature view `seller_stats` ttl 365 d,
  online store Redis (`REDIS_URL` env), offline file, registry `data/feast/registry.db` (local file path from env).
- `train` (config-driven): load training Parquet → drop undelivered → Feast `get_historical_features`
  (entity df: seller_id + event_timestamp = order_approved_ts_utc) → `order_features` → split → fit
  constant prior, logistic regression (one-hot), LightGBM (native categoricals; early stopping on
  validation) → metrics on validation and test (PR-AUC, ROC-AUC, Brier, recall at precision 0.5) → log
  each model as its own MLflow run under experiment `late_delivery` (baselines first) → register the
  LightGBM model, set alias `challenger` → `promote` (rules in Global Constraints; first model with no
  champion promotes). Model = sklearn `Pipeline`/pyfunc that accepts the raw contract columns + seller
  features and applies `order_features` internally (one artefact for batch and online).
- CLI: `retail-ml train late_delivery [--config …] [--promotion-mode auto|manual]`, `retail-ml materialize`.
- Tests: features pure/deterministic, unknown categories (Review Focus 3); leakage guard: Feast join on a
  fixture history returns the snapshot before the approval date (Review Focus 1); synthetic planted-signal
  dataset → LightGBM ROC-AUC ≥ 0.80 (metric floor); promotion rules (better/worse/failed model test/no
  champion/manual mode) with a file-based MLflow (`tmp_path` tracking URI).
- [ ] Red tests, implement, `uv run --project ml pytest`, ruff + mypy (strict on `retail_ml`). Full-data
  run once against the live MLflow (after T1, T2 land; the lead sequences it): report validation/test
  metrics for all three models and which got `champion`. Commit `feat(ml): features, feast repo, late-delivery training and promotion`.

### Task 4: demand forecast and batch scoring

**Owner:** `ml-engineer`. Worktree. Files: `forecast/{features,train,predict}.py`, `late_delivery/score.py`,
`ml/configs/demand_forecast.yaml`, tests `test_forecast*.py`, `test_score.py`.
- Forecast features: lags ≥ 28 only (28, 35, 42, 56), rolling means (7, 28 over the lagged series),
  dow, month, day-of-month, series ids as categoricals → one LightGBM global model forecasts all 28 days
  directly. Backtest 3 folds + test; WAPE overall and per top-10 series; seasonal-naive baseline logged first.
- `retail-ml forecast demand` → `data/gold/ml/pred_demand_forecast.parquet` (`date, product_category,
  customer_state, forecast, model_version, run_ts`) for the next 28 days after the last date, plus the test
  window predictions with actuals (`split = test`) for Power BI Forecast vs Actual.
- `retail-ml score late_delivery` → `data/gold/ml/pred_late_delivery.parquet`: open orders (approved, not
  delivered) scored by the champion with Feast offline features as of now; columns `order_id, probability,
  model_version, scored_ts`.
- Tests: no lag < 28 feature exists; seasonal naive correctness; excluded series reported; scoring uses the
  champion alias and the same `order_features`.
- [ ] Commit `feat(ml): demand forecast and batch scoring`.

### Task 5: FastAPI serving

**Owner:** `ml-engineer`. Worktree. Files: `serving/{app,model,metrics}.py`, `ml/locustfile.py`, tests.
- `GET /health` (`model_loaded`, `model_version`), `POST /predict/late-delivery` (Pydantic request with the
  raw order fields of the contract minus labels/timestamps-after-approval; response `probability,
  model_version`), `POST /reload`, `GET /metrics` (prometheus-client: `requests_total{route,status}`,
  `request_latency_seconds` histogram, `prediction_probability` histogram, `model_version_info`). Online
  seller features from Feast (`get_online_features`), defaults when missing (Review Focus 3). Registry
  empty → 503 (Review Focus 5).
- Tests (TestClient + stub pyfunc + stub feature store): schema contract, 503 without model, unknown seller,
  metrics exposed, reload swaps version.
- Locust: `locust -f ml/locustfile.py --headless -u 20 -r 5 -t 60s --host http://127.0.0.1:8000` → report
  p50/p95 (target p95 < 50 ms with Redis warm).
- [ ] Commit `feat(ml): fastapi serving with feast online features and prometheus metrics`.

### Task 6: monitoring

**Owner:** `ml-engineer`. Worktree. Files: `monitoring/{drift,performance}.py`, tests.
- `retail-ml monitor late_delivery`: reference = champion's training window features + predictions
  (rebuilt from the training Parquet and the champion), current = orders scored in the last 7 days of
  data; Evidently data-drift + prediction-drift report → HTML to `s3://lakehouse/monitoring/late_delivery/
  <date>.html`; delayed ground truth: scored orders now delivered → weekly precision/recall/PR-AUC;
  append rows to `data/gold/ml/ml_monitoring.parquet` (`run_ts, metric, value, window`). Exit code 3 when
  `drift_share > DRIFT_SHARE_THRESHOLD (0.3)` or `pr_auc < PR_AUC_FLOOR (from the champion's test PR-AUC −
  0.05)`; 0 otherwise.
- Tests: synthetic shifted data triggers exit 3; unshifted doesn't; empty current window → 0 with a warning.
- [ ] Commit `feat(ml): evidently drift and delayed ground-truth monitoring`.

### Task 7: ML DAGs

**Owner:** `data-engineer`. Depends on T2-T6. Files under `orchestration/` only.
- `ml_common.ml_task(task_id, args)` = DockerOperator(image `retail-ml`, network `retail_retail`, env,
  mounts `${HOST_REPO_DIR}/data:/data`, `${HOST_REPO_DIR}/ml/feature_repo:/feature_repo`).
- `feast_materialize` (schedule `[GOLD]`), `train_late_delivery` (`@weekly` + triggerable),
  `train_demand_forecast` (`@weekly`), `score_late_delivery` (`@daily`), `forecast_demand` (`@daily`, after
  materialize), `monitor_late_delivery` (`@daily`; on exit code 3 → `TriggerDagRunOperator(train_late_delivery)`;
  manual-approval mode passes `--promotion-mode manual`).
- DAG tests extended (owners, retries, schedules, trigger wiring). Verify: trigger the chain once on dev.
- [ ] Commit `feat(orchestration): ml training, scoring, forecast, materialize and monitoring dags`.

### Task 8: data science artefacts

**Owner:** `ml-engineer`. Depends on T3, T4 full-data runs. Files under `ds/`.
- Notebooks (outputs cleared, run top-to-bottom on dev data via `uv run --project ml jupyter nbconvert
  --execute --to notebook --inplace` then cleared): label balance, lateness by state/category/season,
  distance and freight effects, seller concentration; demand seasonality, sparsity, series selection.
- `ds/reports/iteration_log.md` (baseline → each iteration with metric deltas from MLflow), SHAP summary
  + top features, segment error analysis (by state, category, distance bucket).
- Model cards: intended use, data, metrics vs baselines (validation/test), limitations, ethics, owner.
- [ ] Commit `docs(ds): eda notebooks, iteration log and model cards`.

### Task 9: CI, ADR-0006, runbook, README

**Owner:** `platform-engineer`. Depends on all.
- CI `lint-test`: `uv run --project ml pytest` (incl. synthetic floor + API contract), ruff/mypy for `ml`.
- CI `ingest`: after gold on the sample, `make ml-build` (bake + GHA cache) and a smoke train
  `retail-ml train late_delivery` with a file-based MLflow (no server) asserting the run and artefact exist.
- ADR-0006: ml as separate uv project + one image; Feast file offline/Redis online; aliases over stages;
  promotion rules; DockerOperator for ML tasks; Evidently thresholds; why no DVC.
- Runbook `docs/runbooks/ml.md`; README Phase 4 row and "Run it" ML section.
- [ ] Commit `ci: ml tests and smoke train; docs: ADR-0006, ml runbook`.
