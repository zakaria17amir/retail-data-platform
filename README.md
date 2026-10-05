# Retail Data Platform

An end-to-end, laptop-runnable retail data platform built on the real Olist Brazilian e-commerce
dataset: CDC and streaming ingestion into a Delta Lake lakehouse, dbt-modelled gold marts, batch and
real-time ML with MLOps, and GenAI agents, with an AWS + Snowflake cloud path.

## Highlights

- Nothing is dropped silently. Bad payloads go to a bronze quarantine and silver rule violations to
  `_rejects` + `_rule_metrics`, behind Great Expectations gates
  ([ADR-0003](docs/adr/0003-ingestion-serialization.md), [ADR-0004](docs/adr/0004-silver-design.md),
  [lakehouse runbook](docs/runbooks/lakehouse.md)).
- Incremental silver with idempotent Delta writes, latest-LSN MERGE and CDC-driven SCD2
  ([ADR-0004](docs/adr/0004-silver-design.md)).
- dbt gold star schema, with MetricFlow as the single metric definition, scheduled by Airflow 3
  ([ADR-0005](docs/adr/0005-gold-and-orchestration.md), [analytics runbook](docs/runbooks/analytics.md)).
- Late-delivery model beats its baselines on test: PR-AUC 0.134 vs 0.055 prior / 0.104 logistic.
  Promotion is gated on MLflow champion/challenger plus a Brier ≤ min(prior, logistic) check
  ([ADR-0006](docs/adr/0006-ml-platform.md), [model card](ds/model_cards/late_delivery.md)).
- Real-time ML: CDC stream scoring and Spark session features pushed to Feast for `/recommend`.
  Missed latency targets are documented with measured numbers
  ([ADR-0007](docs/adr/0007-realtime-ml.md), [realtime runbook](docs/runbooks/realtime.md)).
- AWS + Snowflake slice as Terraform, validated in CI; apply is manual
  ([ADR-0009](docs/adr/0009-cloud-slice.md), [cloud runbook](docs/runbooks/cloud.md)).
- Incidents, trade-offs and limitations: [docs/interview-notes.md](docs/interview-notes.md).

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

### GenAI and agents (enrichment, RAG, analytics + shopping agents)

```sh
ollama pull qwen2.5:7b-instruct && ollama pull bge-m3   # host Ollama (GPU); then set LITELLM_MASTER_KEY=sk-... in .env
make up PROFILE=genai                 # LiteLLM gateway :4000 (Ollama by default), Chainlit UI http://127.0.0.1:8010
make enrich                           # 500 products (ENRICH_LIMIT) -> silver/product_enriched
make rag ARGS=init && make rag ARGS=index && make rag ARGS=eval   # pgvector hybrid index, Recall@10 / MRR
set -a; . ./.env; set +a                                          # plain `uv run` does not read .env
uv run --project agents agents shop init                          # shop.stock + insert-only shop_writer role
uv run --project agents agents analytics "What was revenue by year?"
make agents-eval ARGS="analytics --limit 10"                      # golden sets -> MLflow genai-evals
```

The Postgres image moves to pgvector on the existing volume; do the checked swap once first. Measured on
the RTX 4050 (local Qwen, ≈ 20 tok/s): enrichment 196/200 accepted in 12.5 min; RAG Recall@10 0.98 hybrid;
analytics eval execution accuracy 0.70; shopping tool selection 1.0 ([ADR-0008](docs/adr/0008-genai-agents.md)).
Downloads, the swap, hosted mode and limits:
[docs/runbooks/genai.md](docs/runbooks/genai.md).

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
| 0 | Foundation | Done: Compose core, Olist in Postgres, uv workspace, CI |
| 1 | Ingestion | Done: CDC + clickstream into bronze Delta with quarantine ([ADR-0003](docs/adr/0003-ingestion-serialization.md)) |
| 2 | Lakehouse | Done: silver Delta with SCD2, rejects, rule metrics and GE gates ([ADR-0004](docs/adr/0004-silver-design.md)) |
| 3 | Analytics | Done: dbt gold star schema + Parquet marts, MetricFlow metrics, Airflow 3 DAGs, Power BI PBIP ([ADR-0005](docs/adr/0005-gold-and-orchestration.md)) |
| 4 | Batch ML + MLOps | Done: late-delivery risk + demand forecast, Feast, MLflow champion/challenger, FastAPI serving, Evidently monitoring, ML DAGs ([ADR-0006](docs/adr/0006-ml-platform.md)) |
| 5 | Real-time ML | Done: Python CDC stream scorer → `ml.order_risk`, Spark session features + popularity via Feast push, two-stage recommender (co-vis + ALS → LightGBM), `/recommend`, feedback CTR (cold start only so far) ([ADR-0007](docs/adr/0007-realtime-ml.md)) |
| 6 | GenAI & agents | Done: (LiteLLM → Ollama, LLM catalogue enrichment, pgvector hybrid RAG, LangGraph analytics agent over MetricFlow + guarded SQL, shopping agent with human-approved orders, golden-set evals, Chainlit); live pass on local Qwen: 19.4 tok/s enrichment, RAG hybrid Recall@10 0.98 / MRR 0.885, analytics execution accuracy 0.70, shopping tool selection 1.0; DAGs and Chainlit not yet run live ([ADR-0008](docs/adr/0008-genai-agents.md)) |
| 7 | Cloud | code + validation done; apply manual, not yet run — Terraform AWS (S3, EMR Serverless, ECS serving, OIDC, Budget) + Snowflake (Snowpipe, dbt-snowflake gold) ([ADR-0009](docs/adr/0009-cloud-slice.md), [mapping](docs/cloud-architecture.md)) |
| 8 | Polish | Done: interview notes, highlights |

## Data attribution

Brazilian E-Commerce Public Dataset by Olist, licensed under
[CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/). Non-commercial use only.
