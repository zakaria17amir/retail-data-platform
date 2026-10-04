# Retail Data Platform — Master Design Spec

End-to-end data platform (data engineering → analytics → data science → ML/AI engineering) built as a
portfolio project for job applications. This document is the single source of truth for scope,
architecture and decisions. Each phase gets its own implementation plan derived from it.

## 1. Purpose and success criteria

**Purpose.** Prove, with a public GitHub repository, that the author can own the full data lifecycle of a
mid-size online retailer: ingest transactional and behavioural data, build a governed lakehouse and
warehouse, deliver BI, train/serve/monitor ML models in batch and real time, and build evaluated
LLM agents on top — locally reproducible, with a documented cloud deployment path.

**Audience.** Recruiters (README, diagram, demo GIF) and technical interviewers (ADRs, tests, code).

**Success criteria.**
1. A clean clone runs end-to-end with `make up` + `make demo` on a 16 GB laptop (per-layer Compose
   profiles) and comfortably on the author's 24 GB / RTX 4050 Windows machine.
2. Every phase ends in a demoable, tested state with CI green.
3. Every component in the architecture diagram can be explained and defended in an interview; every
   non-obvious choice has an ADR.
4. The README alone is enough to pass a screening call.

## 2. Constraints

- **Local-first.** Docker Compose on Windows (Docker Desktop + WSL2) is the complete system. Cloud is a
  deployable slice plus a mapping document.
- **Hardware.** 24 GB RAM, RTX 4050 6 GB VRAM. `.wslconfig` caps WSL2 at ~16 GB. Ollama runs natively on
  Windows (GPU); containers reach it at `host.docker.internal:11434`.
- **Cost.** Local = free. Cloud demo budgeted at $10-20 total, Snowflake 30-day trial, AWS Budget alarm.
- **Explainability.** Intermediate-level author; prefer one well-understood tool per job over breadth.
- **Licence.** Olist dataset is CC BY-NC-SA 4.0; attributed in README; project is non-commercial.

## 3. Architecture overview

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

Observability and GenAI tracing/evals use MLflow 3 (tracing + GenAI evaluation), Prometheus, Grafana.
Orchestration is Airflow 3 with dataset-aware scheduling. LLM access is through a LiteLLM proxy
(Ollama default, hosted API by config).

## 4. Data

### 4.1 Source dataset: Olist Brazilian e-commerce (real core)

Nine relational tables loaded as-is (raw, uncleaned) into Postgres schema `olist`: `customers`,
`orders`, `order_items`, `order_payments`, `order_reviews`, `products`, `sellers`, `geolocation`,
`product_category_name_translation`. ~100k orders, 2016-2018. Known defects we rely on: duplicated
order items, inconsistent/impossible timestamps, nulls, geolocation outliers, Portuguese categories,
no product names/descriptions, Portuguese review text.

Logical replication enabled; every table gets `updated_at` (trigger) so CDC and SCD2 work.

### 4.2 Replayer (`ingestion/replayer`)

Pushes Olist rows into Postgres in time-compressed chronological order (configurable: 2 years → N
hours), emitting inserts and realistic updates (order status transitions, delivery dates, review
arrival). After the dataset ends it continues by sampling from history with shifted timestamps, so the
system never goes quiet. Modes: `seed` (bulk load, fast) and `live`. Deterministic by seed.

### 4.3 Derived clickstream (`ingestion/clickstream_sim`)

Synthetic browsing sessions grounded in real orders: for each (replayed) order, a session of
`page_view`, `search`, `product_view`, `add_to_cart`, `checkout_started` events preceding the purchase,
plus non-converting sessions from a configurable ratio. Events carry `event_id`, `session_id`,
optional `customer_id`, `device`, `referrer`, `event_ts`. Realism toggles, each justifying a pipeline
feature: late arrival (≤48 h), duplicates (~1 %), schema evolution (new field mid-stream), bad records
(nulls, negative quantities, out-of-range dates). Once the recommender exists, the simulator calls
`/recommend` and emits `recommendation_shown` / `recommendation_clicked` with rank-dependent click
probability (simulated online evaluation, labelled as such).

### 4.4 LLM-enriched catalogue (`genai/enrichment`)

Olist has no product text. An Airflow DAG generates `title`, `description`, `tags` per product from
category + dimensions + top reviews via LiteLLM with JSON-schema output, validated with Pydantic
(language, length), written to `silver/product_enriched/`, idempotent by `product_id` + prompt hash.
Default local run: 5k-product sample; full 32k overnight on GPU.

## 5. Ingestion (bronze)

- **CDC path.** Postgres → Debezium (Kafka Connect) → Redpanda `cdc.olist.<table>` (Debezium envelope,
  Avro via Redpanda Schema Registry) → Spark Structured Streaming `cdc_to_bronze` → Delta
  `bronze/olist/<table>/`, append-only, partitioned by ingest date.
- **Event path.** `clickstream_sim` → Redpanda `events.<type>` (Avro) → same Spark app, query
  `events_to_bronze` → `bronze/events/<type>/`. Watermark 72 h (simulator lateness ≤ 48 h + session
  offsets + margin) on the Kafka timestamp, which carries dataset time; dedupe on `event_id` within
  the watermark. Rows behind the watermark are dropped by Spark and reported per micro-batch
  (`numRowsDroppedByWatermark`, logged as a WARNING); they are not quarantined. Dataset time must
  move forward between runs (or bronze is reset).
- **Quarantine.** Records failing deserialization or basic sanity (null PK, unparseable timestamp) →
  `bronze/_quarantine/<source>/` with raw payload + reason. Nothing is dropped silently, except
  streaming dedupe and watermark drops, which are counted and logged.
- **Why Spark here.** Exactly-once Delta sinks with checkpointing, watermarking, scale; maps to EMR
  Serverless. It is the only Spark job family in the project.
- **Containers.** `postgres`, `redpanda`, `redpanda-console`, `kafka-connect`, `minio`, `spark`
  (single node locally): profile `ingest`. `replayer` and `clickstream-sim` run host-side
  (`make replay`, `make sim`) or as long-running containers in profile `demo`, layered on `ingest`
  (`docker compose --profile ingest --profile demo up`). This keeps `ingest` deterministic for the
  integration test.
- **Tests.** pytest for replayer/sim; integration test: replay 1 day, assert bronze = source + quarantine.

## 6. Lakehouse (silver)

Spark batch jobs (`lakehouse/spark/silver/*`) scheduled hourly, reading bronze incrementally with one
Delta streaming read per bronze table (`trigger(availableNow=True)`, checkpointed, idempotent via Delta
`txnAppId`/`txnVersion`). Bronze is append-only, so Change Data Feed adds nothing; CDF is reserved for
tables with updates (ADR-0004). Silver = cleaned, typed, deduplicated, conformed; one row per entity
version.

**Cleaning rules** are named, pure, unit-tested functions with a `rule_id`:
CDC envelope collapse (apply c/u/d in order, soft-delete flag) · CDC exact-duplicate removal (same
key + LSN; Olist order-item "duplicates" are quantity units and are kept) ·
timestamp parsing/localisation (`America/Sao_Paulo` → UTC + original) and impossibility flags ·
geolocation bounding-box clip + zip-prefix centroids · category translation with `unknown` fallback ·
event casting, `event_id` dedupe, >24 h session rejection. Rejects → `silver/_rejects/<domain>/<table>/`
with `rule_id`, `reason`, `record_json`; per-rule counts in `silver/_rule_metrics`, enabling "rule X
rejected N rows (p %)" reporting. Nothing is dropped silently, except superseded CDC versions of
current-state tables and, for SCD2 tables, changes that alter no tracked column; these are reconciled
by distinct key in the quality gates.

**SCD2** for `products`, `customers`, `sellers` (`valid_from`, `valid_to`, `is_current`) driven by
CDC before/after.

**Layout.** Delta on MinIO `s3a://lakehouse/silver/<domain>/<table>`, date-partitioned where large
(only `events/clickstream`, by `event_date`), Z-ordered on join keys (orders: `customer_id`;
order_items: `product_id`, `seller_id`; clickstream: `session_id`); VACUUM retains 168 h; weekly
`OPTIMIZE`/`VACUUM` Airflow task.

**Quality gates (Great Expectations 1.x on pandas batches read with `deltalake`, no Spark; after each
silver load).** Schema, row-count tolerance vs bronze minus rejects, referential integrity,
distribution checks (price > 0, freight ≤ p99 history), freshness SLA. Critical failures block
downstream dbt. Every check result (critical and warning) is appended to `silver/_dq_results` (Delta)
and feeds a Power BI quality page. No full data catalog (DataHub etc.) — dbt docs + `_dq_results` +
diagram cover lineage; catalog is a stretch item.

**Tests.** Each rule on tiny DataFrames with local Spark in CI; integration test asserts silver
invariants on a replayed day.

## 7. Analytics (gold)

**dbt project `analytics/`**, dbt-duckdb reading silver Delta (`delta_scan`), multi-target
(`local` DuckDB, `snowflake`) from day one: every model compiles for both targets (`dbt parse` in
CI) and dialect gaps live in dispatch macros. dbt-snowflake runs in an isolated, pinned `uv tool`
environment (it would downgrade shared dependencies in the workspace); the Snowflake target is first
executed in Phase 7.

- `staging/` one view per silver table; `intermediate/` order lifecycle durations, sessionisation,
  customer order sequences; `marts/` Kimball star schema:
  facts `fct_orders`, `fct_order_items`, `fct_payments`, `fct_sessions`, `fct_events` (daily agg),
  `fct_reviews`; dims `dim_customer` (SCD2), `dim_product`, `dim_seller`, `dim_date`, `dim_geo`,
  `dim_payment_type`; reporting marts `rpt_daily_sales`, `rpt_customer_360` (RFM, LTV),
  `rpt_product_performance`, `rpt_funnel`, `rpt_delivery_sla`, `rpt_data_quality`.
- `metrics/` dbt semantic layer (MetricFlow): revenue, AOV, conversion, late-delivery rate — one
  definition shared by Power BI, ML and the analytics agent.
- Marts (and the star schema facts/dims) materialised as `external` Parquet, one file per model at
  `${GOLD_DIR:-data/gold}/<model>.parquet` (Power BI reads the folder; avoids
  DuckDB single-writer locks across WSL2); staging/intermediate as DuckDB views/tables; Snowflake as
  clustered tables.
- SCD2 dims join facts point-in-time on `valid_from ≤ fact_ts < valid_to`; a key's first version also
  covers facts before its `valid_from` (CDC-created keys carry processing-time `valid_from`), so no
  fact row is dropped. Revenue = `sum(price + freight_value)` over items of orders not
  `canceled`/`unavailable`, not CDC-deleted and with item revenue > 0; AOV = revenue / those orders.
- Tests/docs: generic key/relationship tests, `dbt-expectations`, dbt unit tests for intermediate
  models; `dbt docs` on GitHub Pages (`docs.yml`) and Elementary (optional) are deferred to Phase 8
  (Polish).

**Airflow 3 (`orchestration/`, a separate uv project locked against Airflow's constraints)**, one
`airflow standalone` container with LocalExecutor (production would split scheduler, API server, DAG
processor and triggerer); Spark tasks via DockerOperator over the host Docker socket; dataset-aware
DAGs: `ingest_health` (15 min),
`silver_hourly` (Spark → GE → publishes `silver`), `gold_daily` (triggered by `silver`; `dbt build`
via Cosmos; publishes `gold`), `lakehouse_maintenance` (weekly). Later DAGs attach to the same
datasets. Failures alert via webhook. DAG integrity tests in CI.

**Power BI (`bi/`).** A PBIP project (TMDL semantic model + PBIR report, text in git so it diffs)
importing the gold Parquet files: Executive, Funnel, Customers (RFM, cohorts), Products/Sellers,
Delivery SLA, Forecast vs Actual, Data Quality. DAX measures mirror the dbt metrics one-to-one. Report
pages, an optional `retail.pbix` export and screenshots are authored by hand in Power BI Desktop; the
README GIF lands in Phase 8 and Snowflake as a second data source with the Phase 7 cloud path.

## 8. Batch ML and MLOps

**Problems.** (1) **Late-delivery risk** — binary classification at order approval (~7 % positive);
features: seller history, seller→customer distance, freight ratio, category, payment, calendar.
Served online and scored in batch. (2) **Daily demand forecast** per category × state, 28-day
horizon, LightGBM global model vs seasonal-naive baseline; batch only, feeds Power BI.
Churn is deliberately excluded (Olist repeat-purchase rate ≈ 3 %).

**Data science (`ds/`).** EDA notebooks (outputs cleared, rendered to docs), baseline → iteration log,
SHAP/feature importance, segment error analysis, one model card per model.

**Feature store — Feast (`ml/features/`).** Feature views as code; offline = gold Parquet/DuckDB with
point-in-time-correct joins; online = Redis, materialised by Airflow. Same definitions reused by the
real-time layer.

**Training (`ml/pipelines/`).** Config-driven sklearn/LightGBM pipelines run by Airflow `train_<model>`:
features → time-based split → train → evaluate → log to MLflow (params, metrics, artefacts, data hash,
git SHA) → register as *challenger*. Promotion to *champion* requires beating the champion on holdout
and passing model tests; automatic by default, human-approval by flag (the demo mode). MLflow backend:
Postgres + MinIO.

**Serving (`ml/serving/`).** FastAPI: `POST /predict/late-delivery` (online features from Feast),
`POST /recommend` (section 9), `/health`, `/metrics`, `/reload`. Pydantic contracts, Locust load test
for p95. Nightly batch scoring DAG → `gold/pred_late_delivery/`.

**Monitoring.** Evidently daily DAG: feature drift, prediction drift, delayed ground-truth performance
(by week) → HTML to MinIO + `ml_monitoring` table. Prometheus/Grafana for service metrics (dashboard
JSON committed). Drift/performance thresholds trigger `train_<model>`.

**CI.** Unit tests for features/pipeline steps; smoke-train on sample with metric floor; API schema
contract test; image build. No DVC (MLflow + Delta time travel suffice).

## 9. Real-time ML

1. **Streaming scoring.** CDC `orders` insert with status `approved` → Spark query `score_orders`
   (model loaded as MLflow pyfunc in-process) → topic `ml.late_delivery_scores` + Delta
   `gold/pred_late_delivery_rt/` → consumer writes `order_risk` back to Postgres. End-to-end latency
   measured.
2. **Session recommendations.** Spark query `session_features` (30-min session window: views,
   categories, last N products, cart adds, dwell) pushed to Redis via Feast push API on a 5 s trigger;
   rolling product popularity (1 h/24 h). Two-stage model: candidates = item co-visitation + implicit
   ALS (nightly batch, top-100 per item in Redis); ranker = LightGBM on session × candidate features
   trained on simulated purchase outcomes. Baseline: popularity. Offline metrics: Recall@10, NDCG@10,
   coverage. `POST /recommend {session_id, k}`: Redis features → candidates → rerank → products with
   enriched titles; p95 < 50 ms target; cold-start → category popularity. Feedback loop via simulator
   gives CTR per model version in Power BI.

Forward note: the events watermark (section 5) assumes dataset-time Kafka timestamps. The
`/recommend` emitter must stamp the triggering event's dataset time; a single wall-clock record
would push the watermark years ahead and drop all subsequent clickstream.

Flink/Kafka Streams deliberately not used (one stream engine; documented as the sub-second choice).
Containers: `redis`, `feast` push server. Profile `realtime`.

## 10. GenAI and agents

- **Gateway.** `litellm` proxy container; default route Ollama (native Windows, GPU); hosted API by
  one-line config. Local models: Qwen2.5-7B-Instruct (tool calling, Portuguese), bge-m3 embeddings.
- **Tracing/evals.** MLflow 3 tracing (auto-instruments LangGraph) and GenAI evaluation; no Langfuse.
- **Catalogue enrichment.** See 4.4.
- **RAG index (Airflow DAG).** Chunks = enriched product doc + review summary + reviews; pgvector in
  the existing Postgres + Postgres full-text; hybrid retrieval with reciprocal-rank fusion; synced via
  dataset trigger; Recall@10 on 100 labelled queries.
- **Framework.** LangGraph; shared `agents/core/`: tool registry (Pydantic schemas), guardrail node
  (PII/injection input check, per-agent tool allowlist, output check), retry-with-feedback on tool
  errors (max 3), Postgres checkpointer memory, structured logs. One Chainlit UI with two profiles.
- **Analytics agent.** Tools `list_metrics`/`query_metric` (MetricFlow; preferred over free SQL),
  `get_model_docs` (dbt manifest), `run_sql` (read-only DuckDB role, gold only, LIMIT, timeout,
  EXPLAIN cost guard), `plot`. Graph: clarify → plan → execute → validate (empty/error/suspicious
  magnitude → self-correct) → answer with SQL + chart. Eval: 60 golden questions, execution accuracy +
  judge on explanation; nightly against Ollama, on demand against API.
- **Shopping assistant.** Tools `search_products` (hybrid RAG), `get_product`, `check_stock`,
  `get_recommendations` (passes chat session as a clickstream session), `get_order_status`,
  `place_order` (writes to Postgres → CDC → lakehouse; gated by human-in-the-loop interrupt). Reviews
  treated as untrusted content. Evals: tool-selection accuracy, faithfulness (judge), adversarial
  refusal set, trajectory length.
- Containers: `litellm`, `chainlit`. Profile `genai`.

## 11. Cloud path: AWS + Snowflake

**Principle.** Local is complete; cloud is a cheap, impressive, tear-down-able slice plus a full
mapping document.

**Deployed slice (batch golden path).** S3 (landing + lakehouse) → EMR Serverless (same silver Spark
jobs) → Snowpipe auto-ingest (S3 event → SQS → Snowflake raw) → dbt-snowflake (gold) → Power BI on
Snowflake. ECS Fargate runs the FastAPI serving image (ECR), model artefacts from S3. Secrets Manager,
CloudWatch, AWS Budget alarm at $25. Cloud DAGs run via scheduled GitHub Actions ("MWAA in production").

**Documented-only mapping (`docs/cloud-architecture.md`).** Redpanda → MSK Serverless/Kinesis;
Debezium → MSK Connect/DMS; MinIO → S3; Airflow → MWAA; Feast online → ElastiCache; MLflow → SageMaker
Experiments/Registry; serving → SageMaker endpoint or ECS; Ollama/LiteLLM → Bedrock; pgvector → Aurora
pgvector/OpenSearch; Grafana → CloudWatch/AMG.

**Terraform.** `terraform/aws/` modules `storage`, `iam` (least privilege, GitHub OIDC role),
`emr-serverless`, `serving` (ECR/ECS/ALB), `observability`, `snowpipe-integration`;
`terraform/snowflake/` database, `RAW/SILVER/GOLD`, XS warehouse 60 s auto-suspend, roles `LOADER`/
`TRANSFORMER`/`REPORTER`, stage, pipes, dbt service user. Remote state S3 + DynamoDB; `plan` in CI,
`apply` manual; `make cloud-up`/`cloud-down`. Evidence (screenshots, query history, cost explorer) in
`docs/cloud-evidence/`.

## 12. Repository, dev experience, CI/CD, docs

```
retail-data-platform/
├── README.md  docker-compose.yml  Makefile  .env.example  pyproject.toml (uv workspace)
├── ingestion/      replayer/ clickstream_sim/ debezium/ schemas/
├── lakehouse/      spark/ (bronze app, silver jobs, rules) quality/ (GE)
├── analytics/      dbt project, metrics/
├── orchestration/  airflow dags/, plugins/
├── ml/             features/ pipelines/ serving/ monitoring/
├── ds/             notebooks/ reports/ model_cards/
├── agents/         core/ analytics_agent/ shopping_agent/ evals/ ui/
├── genai/          enrichment/ rag/
├── bi/             retail.pbip retail.SemanticModel/ retail.Report/ screenshots/
├── terraform/      aws/ snowflake/
├── observability/  prometheus/ grafana/
├── docs/           architecture.md adr/ cloud-architecture.md runbooks/ superpowers/
├── tests/          integration/ e2e/
└── .github/workflows/  ci.yml nightly-evals.yml terraform-plan.yml docs.yml
```

- Compose profiles: `core`, `ingest`, `demo` (long-running replayer + clickstream-sim on top of
  `ingest`; `make demo` will use it), `lakehouse`, `analytics`, `ml`, `realtime`, `genai`,
  `observability`. `make up PROFILE=…`, `make seed`, `make replay`, `make test`, `make demo`
  (scripted 10-minute end-to-end; doubles as the e2e test).
- Tooling: `uv`, Python 3.12, `ruff`, `mypy`, `pre-commit`, `sqlfluff`, `tflint`; all images pinned.
- CI: `ci.yml` (lint/type, per-package unit tests, dbt parse + unit tests, DAG integrity, image
  builds, `terraform validate/plan`; per-profile integration tests via Compose), `nightly-evals.yml`
  (ML smoke-train with floor, agent eval sets), `docs.yml` (dbt docs + GE docs + mkdocs → Pages).
  Tagged releases, `CHANGELOG.md`, images to GHCR.
- Docs: README (diagram source committed, demo GIF, 3-command run, CV-style highlights), ADRs for
  every non-obvious choice, per-layer runbooks, `docs/interview-notes.md` (running "what broke and
  how I fixed it" log).

## 13. Phases

Each phase: own implementation plan → PR series → demoable, tested state, CI green.

| # | Phase | Demo at end | ~Weeks |
|---|-------|-------------|--------|
| 0 | Foundation | repo, Compose `core`, Olist in Postgres, Makefile, CI green | 1 |
| 1 | Ingestion | CDC + clickstream into bronze Delta, quarantine, Redpanda Console | 2 |
| 2 | Lakehouse | silver rules, SCD2, GE gates, reject metrics | 2 |
| 3 | Analytics | dbt gold, Airflow DAGs, Power BI report | 2 |
| 4 | Batch ML + MLOps | MLflow, Feast, FastAPI, Evidently, retraining DAG | 3 |
| 5 | Real-time ML | stream scoring, session features, `/recommend`, CTR loop | 2 |
| 6 | GenAI & agents | enrichment, RAG, both agents, evals, Chainlit | 3 |
| 7 | Cloud | Terraform AWS + Snowflake slice, evidence, mapping doc | 2 |
| 8 | Polish | README/GIF, ADRs, blog posts, CV bullets | 1 |

Phases 0-3 alone are a complete DE portfolio piece; applications can start after them.

## 14. Key decisions (to be expanded into `docs/adr/`)

| Decision | Choice | Rejected | Why |
|---|---|---|---|
| Data | Olist real core + replayer + derived clickstream | fully synthetic; REES46-only | real cleanup story and CDC/streaming story both needed; no public set covers all layers |
| Stream bus | Redpanda | Kafka | identical API, ~3 GB lighter, reviewer-runnable |
| Table format | Delta Lake | Iceberg | best Spark + DuckDB read support today; Iceberg discussed in ADR |
| Engines | Spark (bronze/silver) + dbt-duckdb (gold) | Spark-only; DuckDB-only | right tool per layer; no metastore; Spark keyword where defensible |
| Orchestrator | Airflow 3 | Dagster | name recognition; datasets give the asset-style scheduling |
| BI | Power BI | Metabase/Superset | JD frequency; author on Windows; Parquet folder avoids DuckDB locks |
| Cloud warehouse | Snowflake | Athena/Redshift | "S3 + Snowflake + dbt + Power BI" is a dominant real stack |
| Feature store | Feast | hand-rolled Redis | point-in-time joins and one definition for batch + online |
| Stream engine | Spark Structured Streaming only | Flink | one engine; 5 s micro-batch adequate |
| LLM tracing/evals | MLflow 3 | Langfuse | already running; Langfuse v3 needs ClickHouse |
| LLM backend | Ollama default + LiteLLM switch | API-only | free/offline by default, quality upgrade by config |
| Agents | LangGraph | CrewAI, raw loops | interview credibility, explicit graphs, HITL interrupts |
| ML reproducibility | MLflow + Delta time travel | DVC | one less tool |
| Catalog | none (dbt docs + GE docs) | DataHub/OpenMetadata | heavy; stretch item |

## 15. Out of scope

Churn modelling; full data catalog; Flink; streaming layer deployed to AWS; MWAA; customer-facing
web shop UI beyond the Chainlit assistant; multi-tenant/security hardening beyond least-privilege IAM
and read-only agent roles.

## 16. Risks

- Debezium/Kafka Connect + Spark on Windows/WSL2 memory pressure → profiles, `.wslconfig`, single-node
  Spark.
- 7B local models may be unreliable at tool calling → evals run on both backends; API switch
  documented; prompts kept simple with strict schemas.
- Snowflake trial expiry → capture evidence early in Phase 7; dbt profile remains.
- Scope creep → phase gates; anything not in this spec needs a spec change first.
