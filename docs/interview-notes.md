# Interview notes

Sources for every number below: the ADRs in [docs/adr/](adr/), the runbooks in
[docs/runbooks/](runbooks/), the model cards in [ds/model_cards/](../ds/model_cards/) and the phase
ledgers. GenAI quality and latency numbers are in [ADR-0008](adr/0008-genai-agents.md).

## 1. 60-second pitch

I built a retail data platform that runs on one 16 GB laptop, using the real Olist e-commerce data
(99,441 orders, nine relational tables, 2016-18). A replayer pushes the history into Postgres in
time-compressed order, so Debezium has live CDC traffic, and a simulator derives clickstream from the
same orders. Spark Structured Streaming lands both in Delta Lake. A batch job builds a cleaned silver
layer with SCD2 history, and every dropped row is accounted for in rejects plus rule metrics. dbt
builds a gold star schema with MetricFlow metrics, orchestrated by Airflow 3. On top of that sit
batch ML (late-delivery risk, demand forecast) with Feast, MLflow champion/challenger and drift
monitoring; real-time ML (CDC stream scoring, session recommendations); GenAI agents; and a
Terraform AWS + Snowflake slice. CI includes a full-stack ingest e2e job (CDC → bronze → silver →
realtime), and each non-obvious decision has an ADR. Where a target was missed, the docs give the
measured number.

## 2. Architecture in one paragraph

Diagram: [README → Architecture](../README.md#architecture).
Olist rows go Postgres → Debezium → Redpanda (`cdc.olist.*`), and simulator events go to
`events.*`. Both use Avro and Schema Registry. A Spark streaming app decodes each schema id and writes
bronze Delta on MinIO (Silo fork) with a quarantine table. Hourly Spark batch jobs build silver
(latest-LSN MERGE, CDC-driven SCD2, `_rejects`, `_rule_metrics`), and Great Expectations gates them.
dbt-duckdb builds gold (star schema + Parquet marts + MetricFlow), which feeds Power BI, Feast
(file offline store, Redis online store) and MLflow-tracked training. A FastAPI service serves
`/predict` and `/recommend`. A Python Kafka consumer scores approvals from CDC into `ml.order_risk`,
and `spark-realtime` pushes session and popularity features to Feast. Airflow 3 schedules the batch
side; Prometheus/Grafana and Evidently monitor it. The cloud slice maps silver → EMR Serverless,
then Parquet export → Snowpipe → dbt-snowflake.

## 3. What broke and how I fixed it

1. **Quarantine lost rows from later topics.** *Symptom:* review found that in a micro-batch with
   bad rows from several topics, only the first topic's quarantine rows landed. *Cause:* each topic
   wrote quarantine with the same Delta `txnAppId`/`txnVersion`, so the idempotency guard skipped
   every write after the first. *Fix:* one unioned quarantine write per batch, plus `process_batch`
   tests. `lakehouse_spark/bronze/app.py`, [ADR-0003](adr/0003-ingestion-serialization.md).
2. **Delta txn id collision on checkpoint reset.** *Symptom:* after a checkpoint reset, silver
   silently skipped batches. *Cause:* the txn guard keyed on batch id, and a fresh checkpoint restarts
   at 0. *Fix:* `txnAppId = silver-<checkpoint>-<query id>` (a new namespace per checkpoint lifetime);
   bronze gets `BRONZE_RUN_ID`. `silver/job.py`, [ADR-0004](adr/0004-silver-design.md).
3. **Deletes rejected, deleted rows stayed live.** *Cause:* without `REPLICA IDENTITY FULL`, Debezium
   delete events carry only the key in `before`, so domain rules rejected them. *Fix:* set
   `REPLICA IDENTITY FULL` in `olist_schema.sql` and applied it to the dev DB without a reseed (a
   reseed would re-flood CDC). [ADR-0004](adr/0004-silver-design.md).
4. **Silent CDC redelivery drops.** *Symptom:* a record redelivered in a later batch wasn't flagged:
   the SCD2 anti-join dropped it silently and current-state tables rewrote it. *Fix:* match against
   existing silver too and reject as `cdc_exact_duplicate`, so it is counted.
   `silver/cdc.py` + `silver/job.py`, [ADR-0004](adr/0004-silver-design.md).
5. **48 h dedupe watermark dropped late events.** *Symptom:* about 2-4 % of late clickstream events
   vanished. *Cause:* sim lateness (≤ 48 h) plus session offsets went past the watermark. *Fix:* raise
   it to 72 h and log `numRowsDroppedByWatermark` per batch as a WARNING.
   [ADR-0003](adr/0003-ingestion-serialization.md).
6. **Spark `session_window` in update mode.** *Symptom:* Spark 4.0.4 rejects `session_window` in
   update mode (SPARK-36463), and append mode emits only when a session closes. *Fix:*
   `applyInPandasWithState` keyed by `session_id` (30-min gap), which adds pandas/pyarrow to the Spark
   image. `realtime/sessions.py`, [ADR-0007](adr/0007-realtime-ml.md).
7. **Feast skipped non-newer writes.** *Symptom:* the session e2e test failed and online counts went
   stale with no error. *Cause:* the per-type bronze tables deliver a session's events out of order,
   so the pushed `event_ts` didn't grow, and Feast's Redis store silently drops non-newer writes.
   Popularity had the same bug. *Fix:* strictly increasing `event_ts` (last event + `n_events` ms;
   popularity: hour start + counts µs). [ADR-0007](adr/0007-realtime-ml.md).
8. **Bronze serial per-type writes vs a 2 h watermark.** *Cause:* bronze commits one event type's
   table at a time, about 20 dataset hours apart at 3600×. The first type moved the session watermark
   past the other types' rows, and Spark dropped them. *Fix:* 48 h sessions / 49 h popularity
   watermarks, plus a lagging-source test. [ADR-0007](adr/0007-realtime-ml.md).
9. **`gold_daily` short-circuit counted skipped runs.** *Symptom:* re-review found gold would never
   rebuild. *Cause:* the 20 h guard looked at the last successful DAG run, and a short-circuited run
   counts as success. *Fix:* look up the last successful `publish_gold` task instance; tested with a
   skipped-but-success run. `orchestration/dags/gold_daily.py`, [ADR-0005](adr/0005-gold-and-orchestration.md).
10. **The Brier gate let through models worse than the prior.** *Symptom:* on test, the logistic
    baseline's Brier (0.0533) was worse than the constant prior's (0.0521), so a logistic-only bar
    would promote a model calibrated worse than the base rate. *Fix:* the gate requires test Brier ≤
    min(prior, logistic) (LightGBM 0.0497). `late_delivery/promote.py`, [ADR-0006](adr/0006-ml-platform.md).
11. **Demand tail truncation.** *Symptom:* orders fall from about 250/day in mid-Aug 2018 to about 0
    by 2018-09-03 (the dataset's truncated tail, not real demand). *Fix:* end the series at the last day with
    revenue orders ≥ 20 % of the trailing 28-day mean. Error analysis then found ramp-down days in
    the test window (70/55/56/56 orders/day), so the threshold went to 0.5 (E = 2018-08-23).
    `ml_demand_daily.sql`, [model card](../ds/model_cards/demand_forecast.md).
12. **Serving p95 and multi-worker metrics.** *Symptom:* a single CPU-bound uvicorn process was slow
    (p50 600 / p95 1,100 ms), and about 70 % of the model call was pandas feature code on one row.
    Feast forces Prometheus multiprocess mode. *Fix:* 4 workers, a proper multiprocess collector,
    a numpy one-row path, 1-thread LightGBM: p50 41 / p95 120 ms, 108 req/s. `/reload` now reaches
    only 1 of 4 workers, so switching the champion means restarting `serving`. [ADR-0006](adr/0006-ml-platform.md).
13. **HITL quote recomputed on resume.** *Symptom:* review found that after the LangGraph interrupt,
    the resumed node re-ran and re-quoted, so the order placed could differ from the one approved.
    *Fix:* separate quote → approve → place nodes. The approved quote is inserted unchanged, or the
    order aborts on a stock failure. [ADR-0008](adr/0008-genai-agents.md).
14. **pgvector swap on a live CDC database.** *Risk:* changing the Postgres image under an existing
    volume could invalidate text-index collations or break the replication slot. *Fix:*
    `pgvector:0.8.6-pg16-bookworm` keeps glibc 2.36 (trixie would not). After the swap: collations
    2.36 = 2.36, 0 mismatches, `make status` counts identical, slot active, and a CDC probe reached
    bronze. [ADR-0008](adr/0008-genai-agents.md).
15. **Cloud dbt read partial snapshots.** *Cause:* dbt picked the max export run id after a fixed
    Snowpipe wait, which could pick a partial or stale run. *Fix:* the export writes a manifest,
    `snowpipe_wait` polls until loaded, and dbt gets the exact `export_run_id` var.
    `cloud/snowpipe_wait.py`, `analytics/macros/snowflake_export.sql`, [ADR-0009](adr/0009-cloud-slice.md).

## 4. Trade-offs I'd defend

- **Redpanda over Kafka:** same Kafka API, about 3 GB lighter, with the Confluent-compatible
  registry built in. [Spec §14](superpowers/specs/retail-data-platform-design.md),
  [ADR-0003](adr/0003-ingestion-serialization.md).
- **Delta Lake over Iceberg:** the best Spark + DuckDB read support, idempotent `txnAppId` writes,
  MERGE for SCD2 and time travel as the data version (no DVC). [ADR-0004](adr/0004-silver-design.md),
  [ADR-0006](adr/0006-ml-platform.md).
- **Spark for bronze/silver, dbt-duckdb for gold:** the right tool per layer, with no metastore;
  the same models compile for dbt-snowflake. [ADR-0005](adr/0005-gold-and-orchestration.md).
- **Python stream scorer, not Spark:** the Spark image runs Python 3.10 and can't unpickle the 3.12
  champion. Approve → `ml.order_risk` takes 0.52 s and 1.28 s in two e2e runs. [ADR-0007](adr/0007-realtime-ml.md).
- **MetricFlow Python API in-process for agents:** `dbt-metricflow`'s adapter opened the warehouse
  read-write and loaded S3 secrets, so the agents use the API on a read-only DuckDB. [ADR-0008](adr/0008-genai-agents.md).
- **Local-first LLM:** LiteLLM routes to Ollama by default. Hosted models only run with explicit
  `LLM_PROVIDER=hosted`, with no silent fallbacks, and embeddings always stay local. [ADR-0008](adr/0008-genai-agents.md).
- **Terraform validate-only in CI, manual apply:** no cloud credentials in the dev environment, a $25
  budget and a two-step AWS ↔ Snowflake apply. [ADR-0009](adr/0009-cloud-slice.md).

## 5. Honest limitations

- **Latency targets missed:** `/predict` p95 120 ms and `/recommend` p95 140 ms against 50 ms targets
  (per-request CPU on 4 laptop workers). Session → Feast with all events took 20.4 s (first row
  9.7 s) against the original 20 s target, so the target was amended to 45 s (17.7 s in the final CI
  run). The next lever is parallel bronze writes. [ADR-0006](adr/0006-ml-platform.md),
  [realtime runbook](runbooks/realtime.md#known-limits).
- **Synthetic sessions:** the recommender trains and evaluates on 2,026,193 simulated events. Reranked
  R@10 0.1165 vs popularity 0.1050 is an offline score on simulated behaviour.
  [ADR-0007](adr/0007-realtime-ml.md).
- **Feedback CTR covers cold start only:** the sim calls `/recommend` before the session reaches
  Kafka, so every live impression is `global_popularity`. Rerank CTR per model version is in the
  backlog. [realtime runbook](runbooks/realtime.md#known-limits).
- **Cloud never applied:** Terraform is fmt/validate/tflint-clean with mocked tests. Apply is manual
  and hasn't been run, and GE gates run only locally. [ADR-0009](adr/0009-cloud-slice.md).
- **ML caveats:** demand LightGBM loses to seasonal naive on the top-10 series (WAPE 0.4184 vs
  0.4004), and the v1 champion gate is partly in-sample after the window moved. The drift monitor
  breaches on every run on frozen replay data (drift share 0.3125 > 0.3), so it stays paused by
  default. [Model card](../ds/model_cards/demand_forecast.md), [ADR-0006](adr/0006-ml-platform.md).
- **GenAI:** measured on local Qwen2.5-7B (≈ 20 tok/s on a 6 GB laptop GPU): analytics eval
  execution accuracy 0.70 on 10 cases, and full-text RAG recall only 0.02 (AND-matching tsquery), so
  hybrid search is effectively vector search. The shopping agent emits no product-view events, so its
  recommendations are cold start only. [ADR-0008](adr/0008-genai-agents.md).

## 6. CV bullets

- Built an end-to-end, laptop-runnable retail data platform on real Olist data (99,441 orders, 9
  tables): Debezium CDC + Avro clickstream → Spark Structured Streaming → Delta Lake → dbt. The
  1.41M-row CDC snapshot lands in bronze in about 14 min, and silver + rejects equal the source row
  count for every table.
- Designed an incremental silver layer: latest-LSN Delta MERGE, CDC-driven SCD2, idempotent
  transactional writes, rule-level rejects and Great Expectations gates (63 checks). The full-data
  run takes 142-147 s.
- Modelled a dbt-duckdb gold star schema with 18 Parquet marts (dbt build 198 pass / 3 warn) and
  MetricFlow metrics that match the marts exactly (98,199 revenue orders, AOV 160.24). Verified it
  end to end through Airflow 3 (113/113 tasks).
- Shipped a late-delivery LightGBM model: test PR-AUC 0.134 vs 0.055 prior and 0.104 logistic,
  Brier 0.0497 (prior 0.0521). A 10 % alert budget catches 26.7 % of late orders (lift 2.67).
- Built a global LightGBM demand forecast over 61 series (about 60 % of orders): test WAPE 0.535
  vs 0.635 for seasonal naive, with promotion gated on MLflow champion/challenger aliases.
- Cut FastAPI `/predict` latency from p50 600 / p95 1,100 ms to p50 41 / p95 120 ms (108 req/s,
  0 failures) with multi-worker uvicorn, Prometheus multiprocess metrics and a numpy one-row path.
  Documented that the 50 ms target is still missed.
- Built real-time ML: a CDC stream scorer (approve → risk score in 0.50-1.28 s e2e) and stateful
  Spark session features pushed to Feast, feeding a two-stage recommender (R@10 0.1165 vs 0.1050,
  coverage 0.140 vs 0.024 on synthetic sessions).
- Built local-first GenAI on Ollama behind a LiteLLM gateway: LLM catalogue enrichment (196/200
  schema-valid), pgvector hybrid RAG (Recall@10 0.98, MRR 0.885 on 100 queries), and LangGraph agents
  with a guarded SQL tool and a human-approved `place_order` (approve writes the quoted total, reject
  writes nothing).
- Wrote Terraform for AWS (S3, EMR Serverless, ECS Fargate, GitHub OIDC, $25 budget) and Snowflake
  (Snowpipe, dbt-snowflake gold), validated in CI with mocked module tests. Apply is manual.
