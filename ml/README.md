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
