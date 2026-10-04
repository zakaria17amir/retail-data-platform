# ML runbook (Phase 4, batch ML + MLOps)

Gold Parquet → Feast (file offline, Redis online) → MLflow-tracked training → champion/challenger
aliases → batch scoring, demand forecast, FastAPI serving, Evidently monitoring. Design:
[ADR-0006](../adr/0006-ml-platform.md); CLI, env vars and metric caveats: [ml/README.md](../../ml/README.md).
Gold must exist first (`make gold`, [analytics runbook](analytics.md)). Run from the repo root in Git Bash.

## Start

```sh
make ml-build                      # retail-ml image (CLI + API), GIT_SHA = short HEAD baked in
make up PROFILE=ml                 # MLflow http://127.0.0.1:5000, Redis, serving on SERVING_PORT
make up PROFILE=observability      # Prometheus :9090, Grafana :3000 (needs GRAFANA_ADMIN_PASSWORD)
```

`serving` is healthy without a model (`/health` is liveness; `/predict` returns 503 until a champion
exists). Rebuild with `make ml-build` after each commit so runs carry the right `git_sha` tag.

## Train, score, forecast

`make ml` runs one-off `ml-cli` containers (rebuilt with the current SHA, 2 GiB limit):

```sh
make ml ARGS="materialize"                    # Feast apply + Redis online load (serving reads it)
make ml ARGS="train late_delivery"            # prior, logistic, LightGBM runs; LightGBM registered
make ml ARGS="train demand_forecast"          # seasonal naive + LightGBM global
make ml ARGS="score late_delivery"            # -> data/gold/ml/pred_late_delivery.parquet
make ml ARGS="forecast demand"                # -> data/gold/ml/pred_demand_forecast.parquet
make ml ARGS="monitor late_delivery"          # exit 3 = drift / performance breach
```

Each train registers a new version with alias `challenger`; with `PROMOTION_MODE=auto` (default) it
moves `champion` when it beats the current one on the same test window and passes the model tests. In
`manual` mode it prints the approval command: `make ml ARGS="promote late_delivery --version N"`.

Host-side (`uv run --project ml retail-ml …`) uses the host URLs in `.env`; export them first
(`set -a; . ./.env; set +a`). `MLFLOW_ENABLE_PROXY_MULTIPART_DOWNLOAD=false` is required there: the
tracking server hands out presigned links to `minio:9000`, which resolves only inside Compose.

## Serving

```sh
curl -s http://127.0.0.1:${SERVING_PORT:-8000}/health      # {"model_loaded": …, "model_version": …}
docker compose --profile ml restart serving               # pick up a new champion on all 4 workers
uv run --project ml locust -f ml/locustfile.py --headless -u 20 -r 5 -t 60s --host http://127.0.0.1:${SERVING_PORT:-8000}
```

`POST /reload` reloads only the worker that handles it (1 of 4); restart `serving` to switch the
champion everywhere. Set `LOCUST_SELLER_IDS` to known sellers for warm (Redis-hit) latency numbers.
Measured: p50 41 ms, p95 120 ms, 108 req/s (target p95 < 50 ms not met, see ADR-0006). Grafana's
serving dashboard (home) shows request rate, 5xx share, p50/p95 latency, the prediction probability
heatmap and the loaded model version.

## Airflow

ML DAGs (tag `ml`) start paused; each task is a DockerOperator run of `retail-ml` in the 1-slot pool
`ml`, so the `retail-ml` image must be built and the `ml` profile up.

| DAG | Trigger | Does |
|---|---|---|
| `feast_materialize` | `gold` asset | apply + online load → `FEATURES` |
| `forecast_demand` | `FEATURES` | demand forecast Parquet |
| `train_late_delivery`, `train_demand_forecast` | weekly | train + promote (`PROMOTION_MODE`) |
| `score_late_delivery` | daily | batch scores → `SCORES` |
| `monitor_late_delivery` | `SCORES` | monitor; exit 3 → `breach_alert` + `retrain` |

```sh
make airflow-cli ARGS="dags unpause feast_materialize"   # likewise the other five
make airflow-cli ARGS="dags trigger monitor_late_delivery"
```

`monitor_late_delivery` follows scoring only for a daily cadence: `monitor` scores its own
simulated-live window from the training Parquet, not the batch scores. A breach always calls the
alert webhook; `retrain` triggers `train_late_delivery` unless it succeeded within
`RETRAIN_COOLDOWN_HOURS` (72) or `PROMOTION_MODE=manual`. Monitor thresholds in `.env`
(`DRIFT_SHARE_THRESHOLD`, `MONITOR_WINDOW_DAYS`, …) reach the tasks after `make up PROFILE=analytics`.

## Monitoring output

- Evidently HTML: `s3://lakehouse/monitoring/late_delivery/<date>.html` (`MONITORING_REPORT_ROOT`;
  MinIO console http://127.0.0.1:9001).
- Metrics rows (`run_ts, metric, value, window`): `data/gold/ml/ml_monitoring.parquet`; a failed
  report upload records `report_uploaded = 0` and does not hide a breach.
- Drift test by window size: KS / chi-square under 1,000 current rows, Evidently defaults above.

## Troubleshooting

- `train` OOM: `ml-cli` and DockerOperator tasks are limited to 2 GiB; full-data late-delivery training
  peaks at ~1.3 GiB. Stop the ingest profile before training if the laptop is short of memory.
- `/predict` 503: no `champion` alias yet, or `data/feast/registry.db` missing (run `materialize`).
- Feast registry errors after concurrent runs: only one ML job may write the file registry; outside
  Airflow, do not run two `make ml` commands at once.
- CI smoke-trains the image on the 200-order sample with a sqlite MLflow (no server) and asserts the
  three runs, the baked `git_sha` and a loadable model version; promotion is not asserted there.
