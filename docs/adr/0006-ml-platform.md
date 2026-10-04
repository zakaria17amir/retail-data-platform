# ADR-0006: ML platform — separate uv project, Feast, MLflow aliases, DockerOperator jobs

## Status
Accepted

## Context
Phase 4 adds late-delivery risk (batch + online) and a demand forecast on replayed 2016-18 data, on a
16 GB laptop, scheduled by the Airflow 3 container that cannot install Feast/MLflow/Evidently.

## Decision
- **Packaging:** `ml/` is a separate uv project (own `uv.lock`; Feast/MLflow/Evidently pin pandas/pyarrow
  ranges the workspace can't share) and one image `retail-ml` for the CLI (`retail-ml train|score|…`,
  `ml-cli`) and the API (`serving`), so pickled pipeline classes match. `GIT_SHA` is a build arg baked
  as `ENV` (runtime env overrides) for the lineage tag of runs launched without a checkout.
- **Features:** Feast file offline store on the gold Parquet + Redis online store, file registry.
  The point-in-time join runs in 5k-order chunks (the file store's join of 18M rows OOMed at 2 GiB).
  Same-day snapshot (`feature_ts` = approval day 00:00) counts only deliveries before that day.
- **Registry:** MLflow 3 (Postgres + MinIO) with aliases `champion`/`challenger`, not the deprecated
  stages; `--serve-artifacts` proxy, GenAI job runner off (~1.3 GB). Promotion (`auto` default,
  `manual` = challenger only + `retail-ml promote`): beat the champion re-scored on the same test window
  (PR-AUC / WAPE), no NaN or out-of-range output, late-delivery test Brier ≤ min(constant prior,
  logistic) test Brier — on the full test window logistic (0.0533) is worse than the prior (0.0521),
  so a logistic-only bar would accept a model calibrated worse than the base rate.
- **Orchestration:** ML tasks are DockerOperator runs of `retail-ml` over the host socket, all in a
  1-slot Airflow pool `ml` (the Feast file registry has no lock). A monitor breach (exit 3) alerts and
  retrains at most once per `RETRAIN_COOLDOWN_HOURS` (72); `PROMOTION_MODE=manual` alerts only.
- **Monitoring:** replayed history never labels the scored open orders, so `monitor` builds a
  simulated-live window: the `MONITOR_WINDOW_DAYS` (28) before an anchor (last day with ≥ 20 % of its
  trailing mean volume), scored by the champion with features as of approval. Drift stattest by
  sample size: KS / chi-square (p < 0.05) under 1,000 current rows, Evidently defaults (Wasserstein /
  Jensen-Shannon ≥ 0.1) above — KS flagged 14/16 features as drifted at n ≈ 6.6k. Breach = drift share > 0.3
  or the latest supported week's PR-AUC < champion test PR-AUC − 0.05.
- **Serving:** FastAPI, 4 uvicorn workers, Prometheus multiprocess metrics, 1-thread LightGBM and a
  numpy one-row feature path. Measured (Locust, laptop): p50 41 ms, p95 120 ms, 108 req/s, 0
  failures — the 50 ms p95 target is **missed** (~20 ms CPU per request on a laptop); scaling path is
  more workers/replicas or a lighter model.
- **No DVC:** data versions are the gold Parquet SHA-256 + Delta time travel, logged per MLflow run.

## Consequences
- On the frozen replay data the monitor breaches on every run: drift share 0.3125 > 0.3 (5 of 16
  features) against the fixed training reference (approved < 2018-03-01). A retrain uses the same
  split and data, re-creates the same model and is not promoted, so it cannot clear the breach — once
  unpaused that is a daily alert and a non-promoted version every 72 h. `monitor_late_delivery`
  therefore stays paused by default; for demos keep it paused or set `PROMOTION_MODE=manual` (alert,
  no retrain). Future change: drift alerts only, retrain only on a PR-AUC breach.
- `/reload` reaches 1 of 4 workers; switching the champion means restarting `serving`.
- Drift thresholds differ by window size; the monitor window overlaps the champion's test window, so
  its PR-AUC is a pipeline check, not an independent holdout.
- Rejected: one root workspace (dependency conflicts), MLflow stages, CeleryExecutor/KubernetesPodOperator,
  DVC.
