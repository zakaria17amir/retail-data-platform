# ADR-0007: Real-time ML — Python stream scorer, stateful Spark sessions, Feast push

## Status
Accepted

## Context
Phase 5 adds streaming late-delivery scoring on CDC and session recommendations on the clickstream,
on the same 16 GB laptop, with no real browsing history (Olist has orders only).

## Decision
- **Stream scorer = Python Kafka consumer** (`retail-ml stream-score`), not a Spark query: the Spark
  image runs Python 3.10 and cannot unpickle the 3.12 `retail_ml` champion. Champion pyfunc in-process;
  `cdc.olist.orders` transitions to `approved` (op c/u, before ≠ approved) → Postgres lookup + Feast
  online sellers → `ml.order_risk` (`ON CONFLICT DO NOTHING`: first score wins, redelivery-safe), topic
  `ml.late_delivery_scores`, Delta `gold/ml/pred_late_delivery_rt`; offsets committed after the writes
  (at least once). Debezium `poll.interval.ms` 100 (default 500 was the bottleneck). Measured: burst of
  50 approvals p50 344 / p95 380 ms (Postgres commit → scored); e2e approve → `order_risk` 0.52 s and
  1.28 s (two test runs).
- **Session features from bronze Delta** (`spark-realtime`, union of the 5 shopper event tables, 5 s
  trigger). Spark 4.0 forbids `session_window` in update mode (SPARK-36463), so sessions run in
  `applyInPandasWithState` keyed by `session_id` (30-min gap, 48 h watermark) — adds pandas 2.2.3 /
  pyarrow 20.0.0 to the Spark image. The watermark equals bronze's because bronze commits one event
  type's table at a time (~20 dataset h apart at `REPLAY_SPEED=3600`): with 2 h, the first-committed
  type moved the watermark past the other types' rows and Spark dropped them silently. State lives
  ≤ 48.5 dataset h (under a minute of wall clock at 3600×). Per-type tables deliver a session's events
  out of order and Feast's Redis store silently skips writes whose `event_ts` is not newer, so the
  pushed `event_ts` is last event + `n_events` ms (strictly increasing; Feast TTLs are not applied on
  online reads).
  Popularity: 24 h windows sliding 1 h, 49 h watermark (same reason); `views_1h` = the newest clock
  hour (coarse, not trailing 60 min). Its pushed `event_ts` is that hour's start + `views_24h +
  carts_24h` µs, so every count change is strictly newer (a window max `event_ts` did not move when a
  cart committed after a later view, and Feast dropped the update).
- **Feast push server** (`feast-server`, ≤ 500-row pushes); Redis on a named AOF volume (recreates wiped it).
  Online keys expire after 7 days of wall clock (`key_ttl_seconds: 604800`), because every pushed
  session is a key in the `noeviction` Redis shared with the candidates. The TTL also expires
  `seller_stats`, so `materialize` must run at least weekly.
- **Training on simulated history:** `clickstream-sim generate` derives 2,026,193 events (98,652
  converting + 295,956 browsing sessions) from gold orders in 36 s. Two-stage recommender: co-visitation
  + `implicit` ALS + popularity candidates, LightGBM lambdarank. Test (18,910 sessions ≥ 2018-06-01):
  reranked R@10 0.1165 / NDCG@10 0.0675 / coverage 0.140 vs popularity 0.1050 / 0.0550 / 0.024 —
  synthetic sessions, so this is offline evaluation of simulated behaviour.
- **`/recommend`:** Feast session → Redis candidates → pool → one batch rerank; fallbacks category, then
  global popularity. The pool is a plain-Python port of `candidates.session_pools` (pandas cost 20–26 ms
  of 38), pinned equal by a randomised test. Locust 20 users: p50 42 / p95 140 ms — 50 ms target
  **missed**; per-request CPU (Feast popularity read ~5 ms + predict ~7 ms) saturates 4 workers at ~100 req/s.
- **Feedback:** `recommendation_shown/clicked` carry the trigger's dataset time (watermark-safe);
  CTR per model version in gold (`rpt_recommendation_ctr`, metric `recommendation_ctr`) and Power BI.
  Live feedback currently measures only the cold-start strategy. The sim calls `/recommend` while it
  generates a session, before the session's events reach Kafka (and session → Feast takes 20–45 s),
  so the session is unknown and every impression is `global_popularity` with `model_version` null.
  Rerank CTR per model version needs the sim to call `/recommend` after the session's events are
  online (backlog). Until then that path is covered only by the dbt unit test.
- **Deps:** `implicit` (ml), `polars` (clickstream-sim; already locked), pandas/pyarrow (Spark image),
  `confluent-kafka[avro]`, `psycopg[binary]`, `deltalake` (ml image). ml-cli limit 3g (train peaks 1.98 GiB).

## Consequences
- E2E needs `PROFILE=ingest` + `realtime` (container limits ~16.5 GB, above actual use).
- Online pools exclude only `last_product_ids` (≤ 5); offline pools exclude every viewed product.
- Rejected: Spark `score_orders` (Python mismatch), `session_window` append mode (emits only on close),
  Flink/Kafka Streams (second engine).
