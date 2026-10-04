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
                    FastAPI serving  ◄── Spark stream scoring ─┘
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

## Status

| # | Phase | Status |
|---|-------|--------|
| 0 | Foundation | In progress |
| 1 | Ingestion | In progress: CDC + clickstream into bronze Delta with quarantine ([ADR-0003](docs/adr/0003-ingestion-serialization.md)) |
| 2 | Lakehouse | In progress: silver Delta with SCD2, rejects, rule metrics and GE gates ([ADR-0004](docs/adr/0004-silver-design.md)) |
| 3 | Analytics | Planned |
| 4 | Batch ML + MLOps | Planned |
| 5 | Real-time ML | Planned |
| 6 | GenAI & agents | Planned |
| 7 | Cloud | Planned |
| 8 | Polish | Planned |

## Data attribution

Brazilian E-Commerce Public Dataset by Olist, licensed under
[CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/). Non-commercial use only.
