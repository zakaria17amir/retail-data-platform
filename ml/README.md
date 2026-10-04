# ml — `retail_ml`

Separate uv project (own `uv.lock`, not a root workspace member: Feast/MLflow/Evidently pin
pandas/pyarrow ranges the workspace can't share). Run everything from the repo root:

```sh
uv run --project ml pytest ml
uv run --project ml ruff check ml
uv run --project ml mypy --config-file ml/pyproject.toml ml/src
uv run --project ml retail-ml train late_delivery [--config PATH] [--promotion-mode auto|manual]
uv run --project ml retail-ml promote late_delivery --version N   # manual approval
uv run --project ml retail-ml materialize                          # Feast apply + online load
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

Reading the metrics:
- LightGBM's `val_*` metrics are optimistic: validation picks its early-stopping iteration. Compare
  models on `test_*` (promotion does, re-scoring the champion on the same test rows).
- Splits are contiguous by approval time with no embargo: train orders approved just before
  2018-03-01 can be delivered (and labelled) after validation starts, and seller snapshots near a
  boundary overlap in their 90-day windows. Labels never leak into features (snapshots count only
  deliveries before `feature_ts`), but adjacent windows are not fully independent.
