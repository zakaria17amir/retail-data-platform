# Phase 5 — Real-time ML Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Approved orders are scored by the late-delivery champion within seconds of the CDC event
(score topic, Delta, and an `order_risk` row back in Postgres, with measured end-to-end latency), and
`POST /recommend` serves session-aware product recommendations from streaming session features,
nightly candidates and a LightGBM reranker, with a simulated click feedback loop that yields CTR per
model version in gold and Power BI.

**Architecture:**
- **Streaming scoring** is a Python consumer (`retail-ml stream-score`) in the `retail-ml` image. It
  reads Debezium `cdc.olist.orders`, keeps `approved` transitions, gets seller features from Feast
  online, scores in-process with the champion pyfunc, and writes to Kafka `ml.late_delivery_scores`,
  Delta `gold/ml/pred_late_delivery_rt/` (deltalake) and Postgres `ml.order_risk`.
- **Session features** come from a second Spark Structured Streaming app (`spark-realtime`). It reads
  the bronze events Delta tables (already decoded, deduped and quarantined), aggregates per
  `session_id` and rolling product popularity, and pushes to a Feast feature server (`/push`) on a 5 s
  trigger.
- **The recommender** trains nightly on simulated sessions generated offline for all history:
  candidates = item co-visitation + implicit ALS (top-100 per item in Redis), reranker = LightGBM
  (MLflow `recommender`, champion/challenger), baseline = popularity.
- **The feedback loop.** The simulator calls `/recommend` and emits `recommendation_shown` /
  `recommendation_clicked` events (stamped with the triggering event's dataset time) that flow through
  bronze → silver → gold `rpt_recommendation_ctr`.

**Tech Stack:** confluent-kafka (consumer/producer), Feast (PushSource + feature server), PySpark
4.0.4 Structured Streaming, `implicit` (ALS) or the smallest available equivalent, LightGBM, MLflow,
FastAPI, Redis, deltalake, dbt, Power BI TMDL.

**Spec:** `docs/superpowers/specs/retail-data-platform-design.md` §9 (plus §5 for the events watermark
forward note, §8 for serving/registry rules).

## Global Constraints

- **Ruling, with a spec amendment in T8:** streaming scoring is NOT a Spark query.
  - The Spark image is Python 3.10 without `retail_ml`, and the champion artefact pickles `retail_ml`
    classes built for 3.12.
  - The scorer therefore runs in the `retail-ml` image as a Kafka consumer, with the model in-process,
    which still means no network hop per record. It keeps the spec's latency intent and its
    one-artefact (no skew) intent.
  - Spark stays the only stream engine for event aggregation.
- Every `retail_ml` change obeys Phase 4's constraints:
  - separate uv project, `exclude-newer = 2026-09-27T00:00:00Z`;
  - commands `uv run --project ml pytest ml` and `mypy --config-file ml/pyproject.toml ml/src`;
  - env contract names (`MLFLOW_TRACKING_URI`, `REDIS_URL`, `FEAST_REPO_PATH`, `FEAST_REGISTRY_PATH`,
    `GOLD_DIR`, `PROMOTION_MODE`, `GIT_SHA`);
  - API inputs go only through model artefacts.
- New runtime deps are added by **T3 only** (it owns `ml/pyproject.toml` this phase); others report
  needs.
- All event-time logic uses dataset time:
  - the session window is 30 dataset minutes;
  - popularity windows are 1 h / 24 h of dataset time;
  - feedback events carry the triggering event's `event_ts` and the matching Kafka timestamp
    (Phase 1 watermark).
- Postgres side tables live in schema `ml` (not `olist`) so the `olist_cdc` publication never
  captures them.
- Compose profile `realtime`:
  - new services `feast-server`, `stream-score`, `spark-realtime`;
  - `redis`, `mlflow`, `serving` and the stack they need join the profile;
  - images pinned, memory limits, ports on 127.0.0.1.
- Recommender evaluation is offline on a time split of simulated sessions (train sessions before
  2018-06-01, test after) with Recall@10, NDCG@10 and catalogue coverage@10 vs the popularity
  baseline. The baseline is logged first. Honest framing: the sessions are synthetic.
- Latency targets are measured, not assumed:
  - `/recommend` p95 < 50 ms at 20 Locust users;
  - stream scoring end to end (Debezium `source.ts_ms` → `order_risk.scored_at`) reported as
    p50/p95.
  - A missed target is documented with the bottleneck, like Phase 4.
- Conventional commits; parallel tasks in separate worktrees with disjoint files; per the user's
  directive there are no per-task reviews, only one whole-branch review at the end of the phase.

## Review Focus

1. **Duplicate CDC delivery** (consumer restart re-reads offsets) must not double-insert
   `order_risk` (upsert on `order_id`) or double-count the latency metric (only first-time scores
   count). → T4.
2. **Unknown session / cold start**:
   - no session features → category popularity of the session's last viewed category;
   - no category → global popularity;
   - never an empty list or a 500. → T4.
3. **A product with no candidate list** (new item) gets popularity candidates. → T3, T4.
4. **Feedback events with wall-clock timestamps** would push the bronze events watermark years ahead
   and drop all clickstream. The emitter must stamp dataset time. Test asserts it. → T1.
5. **Session window across a replay restart:**
   - late events within the 48 h bronze watermark still update their session;
   - Spark state uses the 30-min session gap in dataset time, with watermark 2 h dataset time;
   - sessions older than that are final. → T2.

## Data contracts

- `data/gold/ml/sessions_offline.parquet` (T1): one row per event of simulated history — the Avro
  event fields plus `is_purchase_target` (bool: product purchased in that converting session).
  Generated deterministically from the gold ML training rows plus the `Catalogue` (seed `SIM_SEED`).
- Feast views (T3) with entities `session` (`session_id`) and `product` (`product_id`):
  - `session_features` (PushSource `session_push`): `n_events`, `n_product_views`, `n_categories`,
    `last_category`, `last_product_ids` (comma-joined, ≤ 5), `n_cart_adds`, `dwell_seconds`,
    `session_start_ts`, `event_ts`.
  - `product_popularity` (PushSource `popularity_push`): `views_1h`, `views_24h`, `carts_24h`,
    `event_ts`.
  - TTL: 1 day for session, 7 days for product.
- Redis candidates (T3): key `cand:<product_id>` → JSON list of `[product_id, score, source]`
  (≤ 100); `pop:category:<category>` and `pop:global` → JSON lists of product_ids (≤ 100). Written by
  `retail-ml publish-candidates`.
- `POST /recommend` (T4):
  - request `{session_id: str, k: int = 10 (1..50)}`;
  - response `{session_id, items: [{product_id, score, category, title: null}], model_version,
    strategy: "rerank"|"category_popularity"|"global_popularity"}`.
  - `title` stays null until Phase 6 enrichment.
- Kafka `ml.late_delivery_scores` (JSON; key `order_id`):
  `{order_id, probability, model_version, approved_ts, scored_ts, source_ts_ms}`.
- Delta `gold/ml/pred_late_delivery_rt/` (same columns).
- Postgres `ml.order_risk(order_id text primary key, probability double precision, model_version
  text, scored_at timestamptz, source_ts timestamptz, latency_ms double precision)`.
- New event types `recommendation_shown`, `recommendation_clicked` on topics `events.<type>`. Avro v3 =
  v2 + nullable `rank int`, `rec_model_version string`, `rec_strategy string`.

## File Structure

```
ingestion/clickstream_sim/src/clickstream_sim/{offline.py (batch history), feedback.py (recommend client + events)}  (T1)
ingestion/schemas/events/clickstream_event.v3.avsc                                                           (T1)
lakehouse/spark/src/lakehouse_spark/realtime/{sessions.py, popularity.py, push.py, app.py}                   (T2)
ml/src/retail_ml/recommender/{candidates.py, ranker.py, train.py, publish.py}, ml/configs/recommender.yaml    (T3)
ml/feature_repo/features.py (+session/product views, push sources)                                           (T3)
ml/src/retail_ml/serving/recommend.py (+ app routes), ml/src/retail_ml/streaming/score.py                    (T4)
ml/locustfile_recommend.py                                                                                    (T4)
docker-compose.yml (realtime profile), Makefile, .env.example, ml.order_risk DDL (sql/realtime.sql), Grafana  (T5)
lakehouse/spark/src/lakehouse_spark/silver/domain/events.py (new types/columns), analytics/models/... (CTR)    (T6)
bi/retail.SemanticModel/... (CTR table + measure)                                                             (T6)
orchestration/dags/ml_dags.py (+train_recommender, publish_candidates)                                       (T7)
tests/integration/test_realtime.py (marker realtime), .github/workflows/ci.yml                               (T7)
docs/adr/0007-realtime-ml.md, docs/runbooks/realtime.md, README.md, spec §9                                   (T8)
```

Waves: **1** = T1 ∥ T3 ∥ T5 → merge → **2** = T2 ∥ T4 ∥ T6 → merge → **3** = T7 ∥ T8 → final review.

---

### Task 1: Simulator — offline history and feedback loop

**Owner:** `data-engineer`. Files under `ingestion/clickstream_sim/`, `ingestion/schemas/events/`.
- `clickstream-sim generate --out data/gold/ml/sessions_offline.parquet`: for every order in
  `late_delivery_training.parquet` (purchase ts, primary product, items) build its converting session
  plus `SIM_BROWSING_RATIO` browsing sessions with the existing pure generators. Mark
  `is_purchase_target`. Deterministic for a seed; report rows and runtime (target < 5 min).
- Feedback (`clickstream-sim run --feedback`):
  - after each `product_view`, call `RECOMMEND_URL/recommend` (`k=10`, timeout 200 ms; a failure skips
    feedback and never blocks the sim);
  - emit `recommendation_shown` per returned item (rank 1..k);
  - emit `recommendation_clicked` for an item with probability `CLICK_BASE (0.3) / rank`, boosted ×2
    when its category equals the session's category;
  - every feedback event takes the triggering event's `event_ts` and the matching Kafka timestamp
    (Review Focus 4).
- Avro v3 schema + registry registration as for v2. Tests:
  - generator determinism;
  - offline row counts per session type;
  - feedback events carry dataset time;
  - a click probability test with a fixed seed;
  - recommend failure → no feedback events, sim continues.
- [ ] Commit `feat(ingestion): offline session history and recommendation feedback loop`.

### Task 2: Spark realtime app — session features and popularity

**Owner:** `data-engineer`. Files under `lakehouse/spark/src/lakehouse_spark/realtime/` and its tests;
exception: the `spark-realtime` compose service.
- Read the bronze events Delta tables (`readStream` per type, union) and run two queries:
  - `session_features`: grouped by `session_id`, 30-min gap session semantics via
    `session_window(event_ts, "30 minutes")`, watermark 2 h dataset time, output mode update;
  - `product_popularity`: sliding 1 h / 24 h windows on `product_id` from `product_view`/`add_to_cart`,
    watermark 25 h.
- `foreachBatch` → POST the batch to `FEAST_SERVER_URL/push` (`push_source_name`, `df`,
  `to: "online"`) in chunks of ≤ 500 rows. HTTP errors fail the batch (retry); a 4xx with a bad
  payload is logged and the batch is skipped with a metric.
- Trigger 5 s; checkpoints under `_checkpoints/realtime/<query>`.
- Tests (`spark` marker) on in-memory frames: session aggregation values, gap splitting, late event
  inside the watermark updates its session, popularity windows; the push client with a fake HTTP
  server.
- [ ] Commit `feat(lakehouse): spark realtime session features and product popularity`.

### Task 3: Recommender training and Feast definitions

**Owner:** `ml-engineer`. Files: `ml/pyproject.toml`/`uv.lock` (deps: `implicit`, or document the
fallback), `ml/src/retail_ml/recommender/*`, `ml/configs/recommender.yaml`, `ml/feature_repo/features.py`
(new entities, push sources, views; existing `seller_stats` untouched), tests.
- **Candidates:**
  - item co-visitation inside sessions, weighted 1/distance in session order, top-100;
  - implicit ALS on session × product interactions (views 1, cart 3, purchase 5), top-100 by item-item
    similarity;
  - merged with source tags.
- **Ranker:** LightGBM on (session prefix up to the last `product_view` before checkout) × (candidate)
  features:
  - session: counts, last category, dwell;
  - candidate: co-vis score, ALS score, popularity in the prefix's 24 h, same-category flag, price
    band;
  - label = candidate is the purchase target.
- **Baseline:** popularity (top-k most viewed in the train window, category-matched first).
- **Metrics:** Recall@10, NDCG@10, coverage@10 on the test sessions for baseline, candidates-only and
  reranked.
- **Registry:** `recommender` champion/challenger. Promote if test NDCG@10 is higher than the
  re-scored champion and the model tests pass (no NaN scores; coverage ≥ baseline coverage).
- **CLI:**
  - `retail-ml train recommender`;
  - `retail-ml publish-candidates` (writes the Redis keys in the contract from the champion run's
    candidate artefact plus popularity lists).
- Tests: co-visitation correctness on a toy session set; ALS shape; ranker beats popularity on a
  synthetic planted-preference dataset (floor); metrics functions; Redis publish with a fake client.
- [ ] Full-data run after wave 1 merges: report the metrics table. Commit `feat(ml): two-stage
  recommender with candidates, reranker and feast session views`.

### Task 4: Serving `/recommend` and streaming scorer

**Owner:** `ml-engineer`. Files: `ml/src/retail_ml/serving/{recommend.py, app.py routes}`,
`ml/src/retail_ml/streaming/score.py`, `ml/locustfile_recommend.py`, tests.
- **`/recommend`:**
  - Feast online `session_features` + `product_popularity`;
  - candidates from Redis (union over the session's last products, dedupe, exclude already viewed);
  - champion `recommender` rerank (one batch call);
  - cold start per Review Focus 2/3.
  - Metrics: `recommend_requests_total{strategy}`, `recommend_latency_seconds`.
  - The champion is loaded like late delivery (resolved version; 503 only if Redis is down).
- **`retail-ml stream-score`:**
  - confluent consumer group `stream-score`, Avro deserializer for the Debezium envelope;
  - keep `op in (c, u)` where `after.order_status == 'approved'` and `before.order_status != 'approved'`;
  - build the late-delivery raw input from the after image plus `olist.order_items`/payments/geo via a
    small Postgres lookup (same columns as the training contract; document anything unavailable at
    approval);
  - Feast online seller features → champion pyfunc → produce to `ml.late_delivery_scores`, append to
    Delta (batched every 5 s or 500 rows) and upsert `ml.order_risk` (Review Focus 1);
  - Prometheus `stream_score_latency_seconds` (source_ts → scored) on port 8001;
  - commit offsets after the writes.
- Tests: recommend strategies (rerank, category, global) with stubs; the stream filter (only
  transitions to approved); idempotent upsert; latency metric only on first score.
- [ ] Commit `feat(ml): recommend endpoint and streaming late-delivery scorer`.

### Task 5: Realtime infrastructure

**Owner:** `platform-engineer`. Files: `docker-compose.yml`, `Makefile`, `.env.example`,
`sql/realtime.sql` (new; DDL for schema `ml` + `order_risk`, applied by an init service),
`observability/…`.
- **Profile `realtime`:**
  - `feast-server` (retail-ml image, `feast -c /feature_repo serve -h 0.0.0.0 -p 6566`, registry
    read-write from `/data/feast`, 127.0.0.1 port, 512 m);
  - `stream-score` (retail-ml image, `retail-ml stream-score`, env plus Kafka/registry/Postgres, metrics
    port 8001, 1 g);
  - `spark-realtime` (spark image, `spark-submit …/realtime/app.py`, env `FEAST_SERVER_URL`, 3 g);
  - `realtime-init` (creates topic `ml.late_delivery_scores` via rpk and applies `sql/realtime.sql`).
  - postgres/minio/redpanda/redis/mlflow/serving join `realtime`.
- **Make:** `up PROFILE=realtime`, `recommend-load` (Locust recommend).
- **Prometheus:** scrape `stream-score:8001`. **Grafana:** add panels for recommend latency/strategy
  mix and stream-score latency p50/p95.
- `.env.example`: `FEAST_SERVER_URL`, `RECOMMEND_URL`, `CLICK_BASE`, stream batching vars.
- [ ] Verify: config quiet; services start except those needing T2/T3/T4 code (report). Commit
  `feat(platform): realtime profile with feast server, stream scorer and spark realtime`.

### Task 6: Feedback through silver, gold and Power BI

**Owner:** `data-engineer` (silver) + `analytics-engineer` (gold/BI); one worker may do both under
both role files.
- Silver events:
  - accept the two new types and v3 columns (Delta merge schema evolution for new columns);
  - `event_negative_quantity` etc. unchanged;
  - new rule `feedback_missing_rank` (reject shown/clicked without rank).
- Gold:
  - `fct_recommendations` (grain shown event: session, product, rank, model_version, strategy, clicked
    flag via join on session + product + rank);
  - `rpt_recommendation_ctr` (date × model_version × strategy: shown, clicked, CTR, CTR@1-3);
  - MetricFlow metric `recommendation_ctr`.
- TMDL: new table + measure `CTR`, mirroring the dbt metric.
- Tests: silver rule; a dbt unit test for click attribution; reconciliation of CTR between mart and
  metric.
- [ ] Commit `feat(analytics): recommendation feedback ctr in silver, gold and power bi`.

### Task 7: Orchestration, end-to-end test, CI

**Owner:** `data-engineer`.
- DAGs:
  - `generate_sessions` (on GOLD asset, ml_task-like DockerOperator for the sim image) →
    `train_recommender` (weekly + after generate) → `publish_candidates` (after train);
  - all ML containers in the `ml` pool;
  - DAG tests.
- `tests/integration/test_realtime.py` (marker `realtime`, gated by `ALLOW_RESEED=1` like ingest):
  - emit one session through the sim → session features in the Feast online store within 20 s, and
    `/recommend` returns `strategy=rerank`;
  - insert an approved-order transition via `replayer apply_change` → `ml.order_risk` row within 20 s;
  - report the measured latencies.
- CI: a `realtime` job (or an extension of `ingest` if the time budget allows; decide from measured
  durations).
- [ ] Commit `feat(orchestration): recommender dags; test: realtime e2e`.

### Task 8: Docs and spec

**Owner:** `platform-engineer`.
- ADR-0007 (≤ 45 lines): stream scorer as a Python consumer vs a Spark query (the Python 3.10 vs 3.12
  artefact constraint); bronze-Delta-sourced session features; Feast push server; simulated history
  for training; dataset-time feedback stamping; measured latencies.
- Runbook `docs/runbooks/realtime.md`; README Phase 5 row + "Run it"; spec §9 amendments.
- [ ] Commit `docs: ADR-0007 realtime ml, realtime runbook`.
