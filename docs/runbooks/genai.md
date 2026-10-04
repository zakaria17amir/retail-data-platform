# GenAI and agents runbook (Phase 6)

LiteLLM gateway (`litellm`, :4000) → Ollama on the host GPU. Catalogue enrichment → Delta
`silver/product_enriched`; hybrid RAG index in pgvector (`rag.chunks`); analytics and shopping agents
(LangGraph) with a Chainlit UI (`chainlit`, :8010); evals logged to MLflow. Design:
[ADR-0008](../adr/0008-genai-agents.md). Needs silver (`make silver`), gold and the dbt artefacts
(`make gold`; the analytics agent reads `analytics/target/semantic_manifest.json` and `manifest.json`),
and the `retail-ml` image for `serving` (`make ml-build`). Run from the repo root in Git Bash.

**Status:** code and mocked-LLM tests only. No live run has happened yet (no models downloaded), so
this runbook gives the commands and the expected output from the code, not measured numbers.

## 1. Downloads (once)

Ollama runs natively on Windows (not in Compose). Models (sizes from the Ollama library):

```sh
ollama pull qwen2.5:7b-instruct   # ~4.7 GB, Q4_K_M; aliases chat and judge
ollama pull bge-m3                # ~1.2 GB, 1024-dim embeddings; alias embed
ollama list                       # both must be listed
# or: make ollama-models          # pulls both when `ollama` is on PATH, otherwise prints the commands
```

Images pinned in `docker-compose.yml` (the first is also used by `realtime-sql-init`):

```sh
docker pull pgvector/pgvector:0.8.6-pg16-bookworm@sha256:ccc6e83d6e35e931dc7c5def2022729d5a6c370318d099181995567ff1fb4d6b
docker pull ghcr.io/berriai/litellm:v1.101.0@sha256:d295634e09c648dcdb72c4cc2dd226f5fb87823a73e88cbbed6f205e4deb044b
docker compose --profile genai build chainlit   # image retail-agents; base ghcr.io/astral-sh/uv:python3.12-bookworm-slim (digest in agents/Dockerfile)
uv sync --project genai && uv sync --project agents   # host-side CLIs (separate uv projects)
```

`.env`: set `LITELLM_MASTER_KEY` to any value starting with `sk-` (litellm refuses to start without
it: "set LITELLM_MASTER_KEY in .env"), e.g. `sk-$(openssl rand -hex 16)`. `RAG_DSN` and `SHOP_DSN`
must use the same port as `POSTGRES_DSN` / `POSTGRES_PORT`. Leave `LLM_PROVIDER=local`.

## 2. Postgres → pgvector swap (once, keeps the volume)

The `postgres` image changes from `postgres:16.6` to `pgvector/pgvector:0.8.6-pg16-bookworm` (PG 16.15,
same major, same Debian bookworm glibc 2.36, so collations and the data directory stay valid). The
volume `retail_pgdata` is reused; nothing is wiped. Stop writers first (replayer, sim, stream-score).

Pre-checks:

```sh
make status > pre-status.txt
docker compose exec postgres psql -U retail -d retail -c "select slot_name, active, restart_lsn, confirmed_flush_lsn from pg_replication_slots;"
curl -s http://127.0.0.1:8083/connectors/olist-postgres/status   # if the ingest profile is up
```

Swap only postgres:

```sh
docker compose up -d --no-deps --wait postgres
docker compose logs --tail 50 postgres   # expect "database system is ready to accept connections"; STOP on an incompatible data directory
```

Post-checks:

```sh
docker compose exec postgres psql -U retail -d retail -c "select version();" \
  -c "select * from pg_available_extensions where name='vector';" \
  -c "select slot_name, active, restart_lsn, confirmed_flush_lsn from pg_replication_slots;" \
  -c "select datname, datcollversion, pg_database_collation_actual_version(oid) from pg_database;"
make status > post-status.txt && diff pre-status.txt post-status.txt
curl -s http://127.0.0.1:8083/connectors/olist-postgres/status   # not RUNNING: docker compose --profile ingest restart kafka-connect
```

Expect `PostgreSQL 16.15`, a `vector` row, slot `olist_debezium` (turns `active=t` again once Connect
reconnects), `datcollversion` equal to the actual version on every database, and no row-count diff.
CDC smoke test: update one row and check that `confirmed_flush_lsn` advances or the row reaches bronze.
Baseline recorded before the swap: Postgres 16.6, orders 99,441, products 32,951, `ml.order_risk` 151.

## 3. Start

```sh
make up PROFILE=genai
curl -s http://127.0.0.1:4000/health/liveliness
curl -s http://127.0.0.1:4000/v1/models -H "Authorization: Bearer $LITELLM_MASTER_KEY"
curl -s http://127.0.0.1:4000/v1/chat/completions -H "Authorization: Bearer $LITELLM_MASTER_KEY" \
  -H 'Content-Type: application/json' -d '{"model":"chat","messages":[{"role":"user","content":"Say OK"}],"max_tokens":5}'
curl -s http://127.0.0.1:4000/v1/embeddings -H "Authorization: Bearer $LITELLM_MASTER_KEY" \
  -H 'Content-Type: application/json' -d '{"model":"embed","input":"hello"}' \
  | python -c "import sys,json; print(len(json.load(sys.stdin)['data'][0]['embedding']))"   # 1024
```

`genai` starts postgres, minio, minio-init, mlflow-init, mlflow, redis, serving, litellm and chainlit
(no Redpanda). Host-side `uv run` commands below need the `.env` variables: the `make` targets export
them; for plain `uv run`, first `set -a; . ./.env; set +a`.

## 4. Enrichment (default 500 products)

```sh
make enrich                                # ENRICH_LIMIT (500), stratified by category
make enrich ARGS="--limit 50"              # or --category toys, --all (every current product), --batch-size 25
```

Prints `A accepted, R rejected, S skipped in T s: x products/s, y tok/s`. Commits every 25 products,
so a crash loses at most one batch. Idempotent on `(product_id, prompt_hash)`: a re-run reports
everything skipped and makes no LLM calls. The prompt hash covers the product facts, reviews and schema,
so changed reviews re-enrich a product (readers take the latest `enriched_at`). Rejects (after 3
attempts with the validation error fed back) go to `silver/_rejects/product_enriched` with `rule_id`
(e.g. `description:not_english`, `tags:too_short`) and are not retried unless the prompt changes. The
rate estimate behind the 500 default is ~35 tok/s (5k ≈ 8 h, so 500 ≈ 50 min); not measured yet.
Traces go to MLflow experiment `genai` (tags `prompt_hash`, `model_alias`) when `MLFLOW_TRACKING_URI`
is set.

## 5. RAG index

```sh
make rag ARGS=init                          # sql/rag.sql: extension vector, schema rag, rag.chunks, HNSW + GIN; "rag.chunks ready"
make rag ARGS=index                         # summaries (chat) first, then embeddings (embed), batches of 64
make rag ARGS='search "a soft pillow for kids"'   # also --k 10 --category <English name> --mode hybrid|vector|text
make rag ARGS=eval                          # Recall@10 + MRR per mode; MLflow run rag-eval (experiment genai)
```

`index` prints `P products, C chunks, S summaries, R rejected, E embedded, X pruned in T s`; a re-run
should print 0 summaries, 0 embedded, 0 pruned. It indexes only enriched products (doc, one LLM review
summary, reviews). Summaries are made once per product; delete a `review_summary` row to refresh it.
`eval` writes `genai/eval_data/rag_queries.jsonl` (100 synthetic LLM-written queries, product id as
label) only if missing; delete it to regenerate, commit it after the first real run.

## 6. Shopping side tables

```sh
uv run --project agents agents shop init    # sql/shop.sql: schema shop, shop.stock, role shop_writer; "shop.stock rows: 32951"
```

Idempotent. Already applied once to the dev database (32,951 rows, 1,668 = 5.06 % out of stock).

## 7. Agents CLI

```sh
uv run --project agents agents analytics "What was revenue by year?"      # answers clarifications on stdin
docker compose exec postgres psql -U retail -d retail -c "select customer_id from olist.orders limit 3"
uv run --project agents agents shopping "find a soft pillow for kids" --customer <customer_id>
```

Analytics answers end with the metric/SQL citation and a chart path (`CHART_DIR`). Shopping prints the
quote of any order and asks `Approve this order? [y/N]`; only `y` writes (as `shop_writer`, status
`created`). With `POSTGRES_DSN` set, memory is the Postgres checkpointer (schema `agents`); pass
`--thread <id>` to continue a conversation.

## 8. Chainlit UI

http://127.0.0.1:8010, profiles "Analytics" and "Shopping". Each graph node shows as a step (tool calls,
args, SQL, errors); orders show Approve/Reject buttons (timeout = reject). The container mounts
`data/gold` and `analytics/target` read-only.

## 9. Evals

```sh
make agents-eval ARGS="build-analytics"            # rebuild analytics_golden.jsonl from gold (no LLM, ~6 s)
make agents-eval ARGS="analytics --limit 10"       # execution accuracy, judge score, trajectory, seconds
make agents-eval ARGS="shopping --limit 10"        # tool selection, refusal / false refusal, faithfulness
```

Golden sets: `agents/evals/data/` (60 analytics, 40 + 20 shopping). `--limit` round-robins across
categories. Each run prints a metrics JSON and logs to MLflow experiment `genai-evals`. Evals never
place orders (approvals are answered `{"approved": false}`). The judge is the local Qwen unless
`--provider hosted`.

## 10. Hosted mode (optional)

Set `OPENAI_API_KEY` (alias `chat-hosted` = `openai/gpt-4.1-mini`) and/or `ANTHROPIC_API_KEY`
(`judge-hosted` = `anthropic/claude-haiku-4-5`) and `LLM_PROVIDER=hosted` in `.env`, then
`docker compose --profile genai up -d litellm chainlit` (recreated with the new env). Enrichment, RAG
summaries/queries, the shopping CLI and the UI then use `chat-hosted`; evals take
`--provider hosted`. Embeddings always stay on local `embed`; `agents analytics` (CLI) always uses
`chat`. There is no fallback: with a key unset the hosted alias fails with an auth error.

## Troubleshooting

- **GPU memory.** `nvidia-smi` and `ollama ps` show what is loaded. Qwen (~4.7 GB) and bge-m3 (~1.2 GB)
  do not fit 6 GB together; close other GPU apps. If Ollama falls back to CPU, throughput drops sharply.
- **Model swap latency.** The first call after a swap waits for the model to load. Batch jobs avoid
  per-item swaps (`rag index` runs all summaries before any embedding); running enrichment and an
  agent at the same time makes Ollama swap repeatedly.
- **Ollama from containers.** litellm reaches `OLLAMA_BASE_URL`
  (`http://host.docker.internal:11434`). Check from the host: `curl -s http://127.0.0.1:11434/api/tags`.
  An upstream "model not found" through the gateway means the alias reached Ollama but the model is not
  pulled. If the container cannot connect at all, make Ollama listen beyond loopback (Windows user env
  `OLLAMA_HOST=0.0.0.0:11434`, restart Ollama).
- **litellm restarting:** `LITELLM_MASTER_KEY` empty (see the log line above).
- **pgvector tests skip** ("the pgvector `vector` extension is not available") until the swap; litellm's
  import-time `load_dotenv` makes tests pick up `POSTGRES_DSN`/`RAG_DSN` from `.env`.
- **Many `description:not_english` rejects:** the English check (stopword ratio ≥ 0.15) is untested on
  real Qwen output; inspect `silver/_rejects/product_enriched`.

## Known limits

- No live numbers yet (enrichment rate, Recall@10/MRR, eval scores).
- `get_recommendations` emits no `product_view` events, so `/recommend` always serves cold-start
  popularity (`strategy` is returned).
- The UI's Shopping profile sends no customer id (no identity decided), so own-order lookups need the CLI's
  `--customer`. At commit `06451e9`, `agents.shopping_agent.tools` has no `default_registry()`, which the
  UI Shopping profile and `agents eval shopping` import, so both fail until it is added.
- `query_metric` for `aov` without group-by is rejected by the cost guard (estimate 11.2B rows); the agent
  must use `run_sql`.
- Agent orders stay `created` and do not decrement `shop.stock`.
