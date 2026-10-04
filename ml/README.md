# ml — `retail_ml`

Separate uv project (own `uv.lock`, not a root workspace member: Feast/MLflow/Evidently pin
pandas/pyarrow ranges the workspace can't share). Run everything from the repo root:

```sh
uv run --project ml pytest ml
uv run --project ml ruff check ml
uv run --project ml mypy --config-file ml/pyproject.toml ml/src
uv run --project ml retail-ml train late_delivery [--config PATH] [--promotion-mode auto|manual]
uv run --project ml retail-ml train demand_forecast [--config PATH] [--promotion-mode auto|manual]
uv run --project ml retail-ml promote late_delivery|demand_forecast --version N   # manual approval
uv run --project ml retail-ml materialize                          # Feast apply + online load
uv run --project ml retail-ml score late_delivery     # -> $GOLD_DIR/ml/pred_late_delivery.parquet
uv run --project ml retail-ml forecast demand         # -> $GOLD_DIR/ml/pred_demand_forecast.parquet
uv run --project ml retail-ml monitor late_delivery                # exit 3 on drift / PR-AUC breach
```

| Env | Default | Used for |
|---|---|---|
| `GOLD_DIR` | `data/gold` | training Parquet and the Feast `FileSource` (`ml/*.parquet`) |
| `FEAST_REPO_PATH` | `ml/feature_repo` | `feature_store.yaml` + `features.py` |
| `FEAST_REGISTRY_PATH` | `data/feast/registry.db` | Feast file registry |
| `REDIS_URL` | `localhost:6379` | Feast online store; `redis[s]://[user:pass@]host[:port][/db]` is converted to Feast's `host:port,db=N,…` (password must not contain `,`) |
| `MLFLOW_TRACKING_URI` | MLflow default | tracking + registry |
| `PROMOTION_MODE` | `auto` | `auto` or `manual` (case/whitespace-insensitive; anything else fails fast); `manual` sets `challenger` only and prints the approve command |
| `GIT_SHA` | `git rev-parse HEAD` | lineage tag when the image has no `.git` |
| `DRIFT_SHARE_THRESHOLD` | `0.3` | monitor: breach when the share of drifted features exceeds it |
| `MONITORING_REPORT_ROOT` | `s3://lakehouse/monitoring` | monitor: Evidently HTML at `<root>/late_delivery/<date>.html`; `s3://` via `MINIO_ENDPOINT` + `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD` (else the `AWS_*` chain), `file://` or a path writes locally |
| `MIN_CURRENT_ROWS` | `30` | monitor: smaller current window → drift recorded, not gated |
| `MIN_LABELLED` / `MIN_POSITIVES` | `100` / `10` | monitor: support a delayed-ground-truth week needs for its PR-AUC to gate |
| `REFERENCE_SAMPLE_ROWS` | `10000` | monitor: seeded sample of the training window used as reference |
| `MONITOR_WINDOW_DAYS` | `28` | monitor: current window length (days ending at the anchor) and the trailing-mean span of the anchor rule |
| `MONITOR_TAIL_RATIO` | `0.2` | monitor: anchor = last approval day with ≥ this × its trailing mean daily approvals |

Monitoring (`retail-ml monitor late_delivery`) runs a **simulated-live window over replayed
data**. The open orders in `pred_late_delivery.parquet` (the dataset's end) are a thin straggler tail
that never gets labels, so `monitor` does not read them: it builds its current set from
`GOLD_DIR/ml/late_delivery_training.parquet`. Anchor ("now") = the last approval day whose order
count is ≥ `MONITOR_TAIL_RATIO` × the mean daily count over the `MONITOR_WINDOW_DAYS` calendar days
ending on it; current = all orders approved on the `MONITOR_WINDOW_DAYS` days ending at the anchor,
scored by the champion (resolved version) with Feast seller features as of approval. On this data the
window overlaps the champion's test window (`test_start`–`test_end` in `configs/late_delivery.yaml`),
so its PR-AUC is a pipeline check, not an independent holdout. Reference = a seeded sample of the
champion's training window (approved < validation start) rebuilt from the training Parquet + Feast
and scored by the champion — in-sample scores, so `prediction_drift` is biased towards drift and only
reported. Drift uses KS / chi-square (p < 0.05) per feature — Evidently's default distance tests flag
noise at small windows; categorical levels under 5 % of the reference are pooled as `__other__`. With
16 features at p < 0.05, `drift_share > 0.3` needs ≥ 5 drifted columns (≈ 0.1 % by chance if
independent). Run from the host with `MLFLOW_ENABLE_PROXY_MULTIPART_DOWNLOAD=false`: the tracking
server advertises presigned downloads whose URLs point at `minio:9000`, which only resolves inside
Compose. Delayed ground truth = the current orders that have `is_late`, per approval week; breach
when the latest week with ≥ `MIN_LABELLED` labels and ≥ `MIN_POSITIVES` positives has PR-AUC <
champion `test_pr_auc` − 0.05 (smaller or single-class weeks are recorded, not gated). Rows
(`run_ts, metric, value, window`) are appended to `GOLD_DIR/ml/ml_monitoring.parquet` and the breach
decided before the report upload; a failed upload logs and records `report_uploaded = 0`. An empty
current window exits 0 with a warning.

Model `late_delivery` is one pyfunc: raw contract columns + the three `seller_*_90d` features in,
P(late) out (`order_features` runs inside). Shape inputs with
`retail_ml.late_delivery.train.model_inputs` (float numerics, naive-UTC timestamps).

Serving (`uvicorn retail_ml.serving.app:app`): `POST /predict/late-delivery` (the order at approval:
contract fields minus `is_late`, the delivery timestamp and `order_status`; unknown fields → 422),
`GET /health` (liveness: always 200, `model_loaded`/`model_version` in the body), `POST /reload`,
`GET /metrics`. The champion is resolved from the `champion` alias and loaded by version; none →
`/predict` 503. Seller features come from Feast online; an unseen seller gets NaN for all three,
the same value training saw for sellers with no snapshot before approval (LightGBM's learned
missing-value branch), so it is a prediction, not a 500. The Feast registry is only read: if it is
missing, `/predict` returns 503 rather than letting Feast create one. Load test:
`locust -f ml/locustfile.py --headless -u 20 -r 5 -t 60s --host http://127.0.0.1:8000`.

Reading the metrics:
- LightGBM's `val_*` metrics are optimistic: validation picks its early-stopping iteration. Compare
  models on `test_*` (promotion does, re-scoring the champion on the same test rows).
- Splits are contiguous by approval time with no embargo: train orders approved just before
  2018-03-01 can be delivered (and labelled) after validation starts, and seller snapshots near a
  boundary overlap in their 90-day windows. Labels never leak into features (snapshots count only
  deliveries before `feature_ts`), but adjacent windows are not fully independent.

Demand forecast (`demand_forecast`, config `configs/demand_forecast.yaml`):
- Only `is_modelled` series are trained/forecast; the rest are logged per run as
  `excluded_series.csv` (+ `n_series_excluded`). Test window = the last 28 days of `demand_daily`
  (C … E); backtest = the three 28-day folds before C, each refit on data before its fold start.
- Features use lags ≥ 28 only (28/35/42/56, 7- and 28-day means of the lag-28 series, calendar,
  series ids as categoricals), so one Poisson LightGBM forecasts all 28 days directly. The pyfunc
  takes demand rows (`date, product_category, customer_state, orders`) with the horizon's `orders`
  NaN and returns one forecast per row; the seasonal-naive baseline (`y[t − 7·ceil(h/7)]`) is
  logged first with the same interface. Promotion: WAPE lower than the champion's on the same test
  window, no NaN/negative forecasts.
- `is_modelled` is computed by dbt for the final cutoff C, so earlier backtest folds select series
  with slightly later information; the test window is unaffected.
- The registered model is trained on data before C (so its test WAPE is out of sample); `forecast
  demand` applies it to the 28 days after E without refitting.

Batch scoring (`score late_delivery`): open orders = approved, `order_delivered_customer_ts_utc`
null and status not `canceled`/`unavailable`. Seller features are the Feast offline snapshot **as of
each order's approval** (the training point-in-time join), not wall-clock now: the dataset is
historical, so "now" is past every snapshot and would give each order post-approval seller stats.
