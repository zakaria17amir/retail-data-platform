# Retail Data Platform

An end-to-end, laptop-runnable retail data platform built on the real Olist Brazilian e-commerce
dataset: CDC and streaming ingestion into a Delta Lake lakehouse, dbt-modelled gold marts, batch and
real-time ML with MLOps, and GenAI agents, with an AWS + Snowflake cloud path.

## Architecture

```
                 ┌──────────────── SOURCES ────────────────┐
  Olist (Kaggle) │ replayer ──► Postgres (OLTP, olist)     │ clickstream_sim ──► Redpanda events.*
                 └──────────────┬──────────────────────────┘                        │
                                │ Debezium CDC → Redpanda cdc.olist.*               │
                                ▼                                                   ▼
                 ┌──── Spark Structured Streaming (bronze app) ── Delta Lake on MinIO ────┐
                 │  bronze/olist/<table>   bronze/events/<type>   bronze/_quarantine/      │
                 └──────────────────────────────┬───────────────────────────────────────────┘
                                                │ Spark batch (hourly, Airflow) + Great Expectations gates
                                                ▼
                       silver/ (cleaned, typed, deduped, SCD2 dims, _rejects/)
                                                │ dbt-duckdb (local) / dbt-snowflake (cloud)
                                                ▼
                       gold/ (star schema, reporting marts as Parquet, semantic layer)
            ┌───────────────┬───────────────────┼─────────────────────┬──────────────────────┐
            ▼               ▼                   ▼                     ▼                      ▼
        Power BI      Feast offline        ML training            RAG index           Analytics agent
                           │               (MLflow registry)     (pgvector)          (LangGraph, Chainlit)
                           ▼                      │                   │
                   Feast online (Redis) ◄── stream features ──┐       ▼
                           │                      │           │  Shopping agent ──► place_order → Postgres
                           ▼                      ▼           │                                  (full circle)
                    FastAPI serving  ◄── Python stream scorer ─┘
                   (/predict, /recommend)        │
                           ▼                     ▼
                Prometheus/Grafana        Evidently drift → retrain DAG
```

## Run it

```sh
cp .env.example .env
make sync
make up
make download   # needs Kaggle credentials in .env
make seed
make status

# no Kaggle account: load the committed sample instead of download + seed
make seed-sample
```

### Ingest (CDC + clickstream → bronze Delta)

```sh
make up PROFILE=ingest   # Redpanda, Console, Kafka Connect + Debezium, Spark bronze app
make sim SIM_ARGS="--max-events 2000"   # second terminal, first: it only sees orders from now on
docker compose exec redpanda rpk group describe clickstream-sim   # wait for STATE Stable
make replay REPLAY_ARGS="--from 2017-10-02 --until 2017-10-03 --speed 7200"
make status              # Postgres counts + bronze/quarantine Delta counts
```

Replay windows must move forward in dataset time: Spark's events watermark is kept in its
checkpoint, so the clickstream of an earlier window (or of restarted demo containers) is dropped.
Run `make reset-bronze` first ([reset recipe](docs/runbooks/ingestion.md#reset-bronze)).

Redpanda Console at http://127.0.0.1:8080, Spark UI at http://127.0.0.1:4040. Details, reset
and quarantine inspection: [docs/runbooks/ingestion.md](docs/runbooks/ingestion.md).

### Lakehouse (bronze → silver Delta + quality gates)

```sh
make silver      # Spark batch: cleaned, typed silver tables, SCD2 dims, rejects + rule metrics
make quality     # Great Expectations gates; exits 1 on a critical failure
make maintain    # OPTIMIZE ZORDER + VACUUM
make status      # adds silver counts, rejects and "rule X rejected N rows (p %)" lines
```

Details, rejects inspection and reset: [docs/runbooks/lakehouse.md](docs/runbooks/lakehouse.md).

### Analytics (silver → gold marts, metrics, Airflow)

```sh
make gold                   # dbt-duckdb: star schema + Parquet marts in data/gold/, all dbt tests
(cd analytics && PYTHONIOENCODING=utf-8 uv run mf query --metrics revenue,aov --group-by metric_time__month)
make up PROFILE=analytics   # Airflow 3 at http://127.0.0.1:8088, user admin / AIRFLOW_ADMIN_PASSWORD
make airflow-cli ARGS="dags unpause gold_daily"   # then silver_hourly, ingest_health
```

Power BI over the Parquet marts: [bi/README.md](bi/README.md). Details, DAGs and alerts:
[docs/runbooks/analytics.md](docs/runbooks/analytics.md).

### ML (Feast, MLflow, serving, monitoring)

```sh
make ml-build && make up PROFILE=ml   # MLflow http://127.0.0.1:5000, Redis, prediction API
make ml ARGS="materialize"            # Feast online store from gold
make ml ARGS="train late_delivery"    # likewise demand_forecast; champion via MLflow aliases
make ml ARGS="score late_delivery"    # batch scores; `forecast demand` for the 28-day forecast
make ml ARGS="monitor late_delivery"  # Evidently drift + delayed ground truth, exit 3 on breach
make up PROFILE=observability         # Prometheus + Grafana serving dashboard at http://127.0.0.1:3000
```

After a new champion, `docker compose --profile ml restart serving` (`/reload` reaches one of the 4
workers). DAGs, promotion and troubleshooting: [docs/runbooks/ml.md](docs/runbooks/ml.md); EDA,
iteration log and model cards: [ds/](ds/).

### Real-time ML (stream scoring, session recommendations)

```sh
make up PROFILE=ingest && make up PROFILE=realtime   # CDC/bronze + feast-server, stream-score, spark-realtime
uv run --package clickstream-sim clickstream-sim generate   # simulated session history from gold
make ml ARGS="train recommender" && make ml ARGS="publish-candidates"
docker compose --profile realtime restart serving
make sim SIM_ARGS="--feedback"        # live sessions -> Feast; /recommend feedback -> CTR in gold (cold start only)
curl -s -X POST http://127.0.0.1:8000/recommend -H 'content-type: application/json' -d '{"session_id": "<id>", "k": 10}'
```

Approvals replayed through CDC land in `ml.order_risk` (0.52 s and 1.28 s end to end in two e2e runs);
`/recommend` measures p50 42 / p95 140 ms at 20 users (50 ms target missed). Live feedback currently
measures only the cold-start strategy (`global_popularity`): the sim calls `/recommend` before the
session's events reach Kafka, so the session is unknown. Rerank CTR per model version needs the sim
to call `/recommend` after the session's events are online (backlog). Grafana dashboard
"Retail realtime ML", Redis persistence, e2e test and limits:
[docs/runbooks/realtime.md](docs/runbooks/realtime.md).

### Cloud (AWS + Snowflake slice)

```sh
make cloud-validate                   # fmt + validate + tflint for all roots, no credentials
make cloud-bootstrap                  # once: Terraform state bucket + lock table
make cloud-up                         # AWS, then Snowflake; fill the snowflake_* outputs into tfvars, run again
gh workflow run cloud-batch.yml       # EMR silver → Parquet export → Snowpipe → dbt-snowflake gold
make cloud-down                       # destroy everything except the state bucket
```

Nothing has been applied yet; `apply` is a manual step. The slice:
- S3 + EMR Serverless (`emr-spark-8.0.0`);
- Snowpipe → `RETAIL.SILVER` → dbt → `RETAIL.GOLD` → Power BI;
- serving on ECS Fargate behind `enable_serving` (off by default);
- a $25 AWS Budget and a Snowflake resource monitor.

Prerequisites, the two-step apply, the evidence checklist and the cost guard-rails are in
[docs/runbooks/cloud.md](docs/runbooks/cloud.md). The local → AWS mapping with cost estimates is in
[docs/cloud-architecture.md](docs/cloud-architecture.md).

## Status

| # | Phase | Status |
|---|-------|--------|
| 0 | Foundation | In progress |
| 1 | Ingestion | In progress: CDC + clickstream into bronze Delta with quarantine ([ADR-0003](docs/adr/0003-ingestion-serialization.md)) |
| 2 | Lakehouse | In progress: silver Delta with SCD2, rejects, rule metrics and GE gates ([ADR-0004](docs/adr/0004-silver-design.md)) |
| 3 | Analytics | In progress: dbt gold star schema + Parquet marts, MetricFlow metrics, Airflow 3 DAGs, Power BI PBIP ([ADR-0005](docs/adr/0005-gold-and-orchestration.md)) |
| 4 | Batch ML + MLOps | In progress: late-delivery risk + demand forecast, Feast, MLflow champion/challenger, FastAPI serving, Evidently monitoring, ML DAGs ([ADR-0006](docs/adr/0006-ml-platform.md)) |
| 5 | Real-time ML | In progress: Python CDC stream scorer → `ml.order_risk`, Spark session features + popularity via Feast push, two-stage recommender (co-vis + ALS → LightGBM), `/recommend`, feedback CTR (cold start only so far) ([ADR-0007](docs/adr/0007-realtime-ml.md)) |
| 6 | GenAI & agents | Planned |
| 7 | Cloud | code + validation done; apply manual, not yet run — Terraform AWS (S3, EMR Serverless, ECS serving, OIDC, Budget) + Snowflake (Snowpipe, dbt-snowflake gold) ([ADR-0009](docs/adr/0009-cloud-slice.md), [mapping](docs/cloud-architecture.md)) |
| 8 | Polish | Planned |

## Data attribution

Brazilian E-Commerce Public Dataset by Olist, licensed under
[CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/). Non-commercial use only.
