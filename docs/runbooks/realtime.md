# Real-time ML runbook (Phase 5)

CDC approvals → `stream-score` (Python consumer, late-delivery champion) → `ml.order_risk`, topic
`ml.late_delivery_scores`, Delta `gold/ml/pred_late_delivery_rt`. Clickstream → bronze Delta →
`spark-realtime` (session features + product popularity) → `feast-server` push → Redis →
`POST /recommend` (two-stage recommender champion). Design and measured numbers:
[ADR-0007](../adr/0007-realtime-ml.md). Needs gold (`make gold`) and the Phase 4 setup
([ml runbook](ml.md)): `make ml-build`, `make ml ARGS="materialize"`, a `late_delivery` champion.
Run from the repo root in Git Bash.

## Start

```sh
make up PROFILE=ingest     # Debezium CDC + bronze Spark app: the realtime profile has no producers
make up PROFILE=realtime   # feast-server :6566, stream-score metrics :8001, spark-realtime, serving
```

`realtime` also starts postgres, minio, redpanda, mlflow, redis and serving, and runs two one-shot
inits: `realtime-init` (topic `ml.late_delivery_scores`) and `realtime-sql-init` (`sql/realtime.sql`:
schema `ml`, table `ml.order_risk`). Container limits add up to ~16.5 GB with `ingest`; actual use is
far lower. `spark-realtime` waits (log "waiting for Delta tables") until the bronze tables of all 5
shopper event types and silver `catalog/products` exist; run `make sim` and `make silver` once first.
On a fresh checkpoint its first batch reads all of bronze history.

## Sessions, training, candidates

```sh
uv run --package clickstream-sim clickstream-sim generate   # -> data/gold/ml/sessions_offline.parquet
make ml ARGS="train recommender"       # popularity, candidates, lightgbm runs; registers `recommender`
make ml ARGS="publish-candidates"      # champion's cand:<product> + pop:* lists -> Redis
docker compose --profile realtime restart serving   # serving loads both champions at start
```

Full data: 2,026,193 events (98,652 converting + 295,956 browsing sessions) in 36 s; training 4 m 16 s
at a 1.98 GiB peak (ml-cli limit 3g); publish writes 32,951 `cand:*` + 74 `pop:*` keys (~197 MB in
Redis). `PROMOTION_MODE=manual` → `make ml ARGS="promote recommender --version N"`. Airflow: DAG
`generate_sessions` runs on the `gold` asset (image `retail-clickstream-sim`; `docker compose build
clickstream-sim` after sim changes), `train_recommender` on its `sessions_offline` asset (train 3g →
publish); both start paused, asset-triggered only.

## Recommend

```sh
curl -s -X POST http://127.0.0.1:${SERVING_PORT:-8000}/recommend \
  -H 'content-type: application/json' -d '{"session_id": "<id>", "k": 10}'
curl -s -X POST http://127.0.0.1:${FEAST_PORT:-6566}/get-online-features -H 'content-type: application/json' \
  -d '{"features": ["session_features:n_events"], "entities": {"session_id": ["<id>"]}}'
LOCUST_SESSION_IDS=<id1>,<id2> make recommend-load   # 20 users, 60 s; LOCUST_UNKNOWN_SHARE default 0.2
```

`strategy` is `rerank` (session has `last_product_ids` and a champion exists), else
`category_popularity` (the session's `last_category`, Portuguese `product_category_name`), else
`global_popularity`; never empty. 503 means Redis is down or candidates are unpublished. `title` is
null until Phase 6 enrichment. Live sessions come from `make sim` (session ids are in the
`events.*` topics); `make sim SIM_ARGS="--feedback"` also calls `/recommend` after each product view
and emits `recommendation_shown/clicked` at the trigger's dataset time → `make silver && make gold` →
`rpt_recommendation_ctr` / metric `recommendation_ctr`. These rows are all `global_popularity` with
`model_version` null: the sim calls `/recommend` before the session's events reach Kafka (see Known
limits). Measured at 20 users: p50 42 / p95 140 ms (target 50 ms missed; at 5 users p50 24 / p95
54 ms).

## Stream scoring

`stream-score` consumes `cdc.olist.orders`; a status change to `approved` (`make replay …` windows
contain them) is scored at once.

```sh
docker compose exec postgres psql -U retail -d retail -c \
  "SELECT order_id, probability, model_version, latency_ms FROM ml.order_risk ORDER BY scored_at DESC LIMIT 5"
docker compose exec redpanda rpk topic consume ml.late_delivery_scores -n 3
curl -s http://127.0.0.1:${STREAM_SCORE_PORT:-8001}/metrics | grep stream_score_latency_seconds_count
```

Delivery is at least once: a redelivered approval is re-sent to Kafka and Delta, but `order_risk`
keeps the first score and the latency histogram counts only first-time scores. A first start of the
`stream-score` consumer group begins at the topic end (no history). After a new `late_delivery`
champion: `docker compose --profile realtime restart stream-score` (champion loaded at start).

## Grafana

`make up PROFILE=observability`, then dashboard "Retail realtime ML"
(http://127.0.0.1:3000/d/retail-realtime): recommend latency p50/p95, strategy mix, stream scoring
latency p50/p95. spark-realtime is not scraped (push skips are log lines only).

## Redis persistence

Redis runs `--appendonly yes` on the named volume `redisdata`, so restarts and recreates keep the
online store and candidates (`docker compose exec redis redis-cli dbsize` before/after;
`redis-cli info persistence | grep aof_enabled` → 1). After `make destroy` (drops volumes) rerun
`make ml ARGS="materialize"` and `make ml ARGS="publish-candidates"`.

Feast online keys (sessions, popularity, `seller_stats`) expire 7 days of wall clock after their last
write (`key_ttl_seconds` in `ml/feature_repo/feature_store.yaml`). `cand:*` / `pop:*` have no TTL.
Run `make ml ARGS="materialize"` (or DAG `feast_materialize`, which runs on each gold publish) at
least weekly, otherwise `stream-score` reads null seller features. Within those 7 days Redis still
grows: a fresh spark-realtime checkpoint pushes every session in bronze, and each
`replayer live --loop` iteration adds a new set of session ids. Redis is `noeviction` at 640 MB, so
when it is full Feast `/push` returns 500 (the spark-realtime query dies) and `publish-candidates`
fails. Check with `docker compose exec redis redis-cli info memory | grep used_memory_human`. To
recover: `docker compose exec redis redis-cli flushdb`, then `make ml ARGS="materialize"`,
`make ml ARGS="publish-candidates"`, and `docker compose --profile realtime restart spark-realtime`
if its query died. Live sessions and popularity then refill from the stream.

## E2E test

```sh
ALLOW_RESEED=1 make test-realtime   # both profiles up
```

Writes test rows to the stack: a session through Kafka → bronze → Feast (asserts `/recommend` reranks;
target 45 s) and an `approved` transition → `ml.order_risk` (target 20 s), then deletes the order rows.
Without `ALLOW_RESEED=1`, or with feast-server / serving unreachable, it skips. CI runs it in the ingest
job on the sample data. Measured approve → `order_risk`: 0.52 s (scorer 143 ms); session → Feast with
all events counted: 20.4 s (first row 9.7 s). Bronze writes each event type's Delta table in turn
(~20 s per micro-batch); writing them in parallel is the next latency lever.

If the host is overloaded (long Spark tests, image builds), Redpanda can stall and the Debezium
connector drop to `UNASSIGNED`; `docker compose --profile ingest restart kafka-connect` recovers it.

## Known limits

- `/recommend` p95 140 ms vs the 50 ms target: per-request CPU (Feast popularity read ~5 ms, predict
  ~7 ms) saturates 4 single-threaded workers at ~100 req/s.
- Recommender metrics are on synthetic sessions (reranked R@10 0.1165 vs popularity 0.1050).
- `views_1h` is the newest clock hour, not a trailing 60 minutes.
- Online pools exclude only `last_product_ids` (≤ 5); offline pools exclude every viewed product.
- At approval the replayer has often not yet written `order_payments` (payment features null);
  seller features are the Feast online snapshot, not point-in-time.
- Replay windows must move forward in dataset time (bronze and spark-realtime watermarks).
- spark-realtime watermarks are 48 h for sessions (the bronze watermark) and 49 h for popularity
  (`SESSION_WATERMARK` / `POPULARITY_WATERMARK`).
  Bronze commits one event type's table at a time, ~20 dataset h apart at `REPLAY_SPEED=3600`, so a
  shorter watermark silently dropped the later-committed types' rows. Session state lives ≤ 48.5
  dataset h: under a minute of wall clock at 3600×, but 2 days of open sessions at 1×.
- Popularity pushes use `event_ts` = newest clock hour + `views_24h + carts_24h` µs, so every count
  change is strictly newer and Feast keeps it.
- Live feedback measures only the cold-start strategy. `make sim SIM_ARGS="--feedback"` calls
  `/recommend` while it generates a session, before the session's events reach Kafka (and
  session → Feast takes 20–45 s), so the session is unknown and the strategy is `global_popularity`.
  Rerank CTR per model version needs the sim to call `/recommend` after the session's events are
  online (backlog).
- Feedback events count in `fct_sessions.event_count` / `fct_events`.
- Feast online keys expire after 7 days of wall clock; re-materialize `seller_stats` at least weekly
  (see Redis persistence).
