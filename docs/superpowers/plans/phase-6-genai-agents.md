# Phase 6 — GenAI and Agents Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An LLM-enriched catalogue, a hybrid RAG index in pgvector, and two LangGraph agents served
through one Chainlit UI, all running locally on Ollama behind a LiteLLM gateway, with MLflow tracing
and offline evals:
- an analytics agent over the semantic layer and gold;
- a shopping assistant over RAG, recommendations, orders and a human-approved `place_order`.

**Architecture:**
- **Gateway.** `litellm` proxy container. Model aliases:
  - `chat` → `ollama/qwen2.5:7b-instruct`;
  - `embed` → `ollama/bge-m3`;
  - `judge` → `chat` by default, or a hosted model when `LLM_PROVIDER=hosted` and an API key is set.
  - Ollama runs natively on Windows with the GPU, reached at `host.docker.internal:11434`.
- **genai/** (one uv project `genai`):
  - `enrichment` (catalogue title/description/tags → `silver/product_enriched` Delta);
  - `rag` (chunk → embed → `rag.chunks` in pgvector + Postgres full-text; hybrid search with
    reciprocal-rank fusion; Recall@10 eval).
- **agents/** (one uv project `agents`):
  - `core` (tool registry, guardrails, retry-with-feedback, Postgres checkpointer, MLflow tracing);
  - `analytics_agent`;
  - `shopping_agent`;
  - `evals`;
  - `ui` (Chainlit, two chat profiles).
- **Postgres image** becomes `pgvector/pgvector` (same major 16), so RAG lives in the existing database
  (schema `rag`). The shopping side tables live in schema `shop`, outside the `olist_cdc` publication.

**Tech Stack:** LiteLLM (proxy + client), Ollama (Qwen2.5-7B-Instruct, bge-m3), Pydantic, pgvector,
psycopg, deltalake, LangGraph (+ langgraph-checkpoint-postgres), MLflow 3 tracing/GenAI evaluation,
Chainlit, DuckDB, MetricFlow.

**Spec:** `docs/superpowers/specs/retail-data-platform-design.md` §4.4 and §10.

## Global Constraints

- **Local-first.** Everything runs against Ollama through LiteLLM.
  - Hosted models are one env switch (`LLM_PROVIDER=hosted` + key) and never required.
  - Tests never call a real LLM. They use LiteLLM `mock_response` or a fake client, plus a
    deterministic fake embedder.
- **GPU budget (RTX 4050, 6 GB).** Qwen2.5-7B Q4 needs about 4.7 GB and bge-m3 about 1.2 GB, so Ollama
  swaps them.
  - Batch jobs never interleave chat and embed per item: enrich all, then embed all.
  - **Ruling:** the default local enrichment is **500 products** (stratified by category). The spec's
    5k sample is about 8 h at ~35 tok/s. `--limit` and `--all` are flags, and the full 32k run is
    documented as overnight.
- **Separate uv projects** `genai/` and `agents/`, like `ml/`:
  - own lockfiles, `exclude-newer = 2026-09-27T00:00:00Z`;
  - commands `uv run --project genai …` / `uv run --project agents …`;
  - mypy via `--config-file <proj>/pyproject.toml`;
  - not root workspace members, so LangGraph/MLflow/LiteLLM can't disturb the dbt/Spark lock.
  - `agents` depends on `genai` through a path dependency for retrieval.
- **Structured outputs** use a JSON schema validated by Pydantic. An invalid output is retried with the
  validation error fed back (max 3), then recorded as a reject with a reason, never silently dropped.
- **Guardrails** (`agents/core`):
  - an input check for PII patterns (CPF, email, phone, card) and prompt-injection markers;
  - a per-agent tool allowlist;
  - an output check: no PII echo, and analytics numbers must come from tool results.
  - Reviews and product text are untrusted data (delimited, never instructions).
- **Data access safety:**
  - `run_sql` uses a read-only DuckDB connection over `data/gold/*.parquet`, SELECT only (parsed), a
    LIMIT enforced (≤ 1000), a timeout of 10 s, and an EXPLAIN-based row estimate guard.
  - `place_order` uses a LangGraph `interrupt` that needs explicit human approval in the UI, then writes
    through a dedicated Postgres role limited to `olist.orders`/`order_items`/`order_payments` inserts.
- **Tracing.** MLflow 3 tracing for every agent run and batch LLM call (experiment `genai`), with the
  prompt hash and model alias tagged.
- **Evals are honest.** Golden sets live under `agents/evals/data/`:
  - analytics: 60 questions with expected answers computed from gold by a script, not hand-typed
    numbers;
  - shopping: 40 tool-selection cases + 20 adversarial cases;
  - RAG: 100 labelled queries generated from enriched docs with the target product as label (synthetic,
    stated).
  - Reported on Ollama; hosted results only if a key exists.
- Compose profile `genai` with services `litellm` and `chainlit`; postgres/minio join the profile.
  Images pinned by digest, memory limits, ports on 127.0.0.1.
- Conventional commits; parallel tasks in separate worktrees with disjoint files. Per the user's
  directive there are no per-task reviews, only one whole-branch review at the end of the phase.

## Review Focus

1. **Idempotency.**
   - Enrichment skips `(product_id, prompt_hash)` already present.
   - The RAG index upserts on `chunk_id = hash(source, source_id, text)`.
   - Re-runs add nothing.
2. **Injection.** A review containing "ignore previous instructions…" must not change agent behaviour;
   it is in the adversarial set.
3. **`place_order` without approval** writes nothing. A rejected interrupt ends with no rows. Test with
   the checkpointer.
4. **`run_sql`** rejects DDL/DML, multiple statements, file functions (`read_csv`, `COPY`, `ATTACH`,
   `INSTALL`/`LOAD`) and over-limit queries.
5. **Analytics answers** cite the metric/SQL used, and numbers match tool output (output check).

## Data contracts

- **Delta `silver/product_enriched/`:**
  - columns: `product_id`, `title` (≤ 80 chars, English), `description` (40–600 chars, English),
    `tags` (array<string>, 3–8, lowercase), `language` (= "en"), `model`, `prompt_hash`,
    `enriched_at`;
  - rejects go to `silver/_rejects/product_enriched` with `rule_id` and `reason`.
  - Source inputs: the silver products (current), the category translation, and the top-3 helpful
    reviews by length (silver reviews).
- **pgvector `rag.chunks`:**
  - columns: `chunk_id text pk`, `source text` (`product_doc`|`review_summary`|`review`),
    `product_id text`, `text text`, `embedding vector(1024)`, `tsv tsvector` (generated, simple
    config), `updated_at`;
  - indexes: HNSW (cosine) on embedding, GIN on tsv.
- **Retrieval API** (`genai.rag.search(query, k=10) -> list[Hit]`): `Hit{product_id, chunk_id, source,
  text, score, rank_vector, rank_text}`. RRF with `k=60`, deduped to products for `search_products`.
- **Postgres `shop.stock(product_id pk, on_hand int, updated_at)`:** deterministic from 90-day sales
  velocity (seeded), created by the shopping task's migration.
- **Agent tool schemas** are Pydantic models in `agents/*/tools.py`. Each tool is registered with name,
  description, input/output models and an allowlist.

## File Structure

```
docker-compose.yml (genai profile, pgvector image), litellm/config.yaml, Makefile, .env.example     (T1)
genai/pyproject.toml, genai/src/genai/{llm.py, enrichment/*.py}, genai/tests/                     (T2)
genai/src/genai/rag/{chunks.py, embed.py, store.py, search.py, eval.py}, sql/rag.sql              (T3)
agents/pyproject.toml, agents/src/agents/core/*, agents/src/agents/analytics_agent/*               (T4)
agents/src/agents/shopping_agent/*, sql/shop.sql                                                   (T5)
agents/src/agents/{evals/*, ui/app.py}, agents/evals/data/*                                       (T6)
orchestration/dags/genai_dags.py (+tests), serving titles join, .github/workflows/ci.yml           (T7)
docs/adr/0008-genai-agents.md, docs/runbooks/genai.md, README.md, spec §4.4/§10                    (T8)
```

Waves: **1** = T1 ∥ T2 ∥ T4 → merge, then a real enrichment run → **2** = T3 ∥ T5 ∥ T6 → merge →
**3** = T7 ∥ T8 → final review.

---

### Task 1: GenAI infrastructure

**Owner:** `platform-engineer`.
- **Postgres:**
  - Swap the image to `pgvector/pgvector:<0.8.x>-pg16` pinned by digest, on the same data volume
    (16.x minor upgrade, compatible). Verify CDC, the Debezium slot and the seed counts survive the
    swap.
  - `CREATE EXTENSION vector` happens in `sql/rag.sql` (T3), not here; just confirm
    `select * from pg_available_extensions where name='vector'`.
- **`litellm`:**
  - pinned image and `litellm/config.yaml` with the `chat`/`embed`/`judge` aliases;
  - `OLLAMA_BASE_URL=http://host.docker.internal:11434`;
  - hosted fallback entries read keys from env (unset = disabled);
  - master key from `.env` (required, runtime guard);
  - 127.0.0.1 port 4000, 1 g.
- **`chainlit`:** built from `agents/Dockerfile` (T4 creates it — reference it, build later); port 8010;
  depends on litellm/postgres. Profile `genai` also includes postgres, minio, mlflow, serving, redis.
- **Make:**
  - `make ollama-models` (prints/pulls `qwen2.5:7b-instruct`, `bge-m3` on the host);
  - `make enrich ARGS=…`, `make rag ARGS=…`, `make agents-eval ARGS=…` (run the projects' CLIs on the
    host with the right env);
  - `make up PROFILE=genai`.
- `.env.example`: `LITELLM_MASTER_KEY`, `LITELLM_URL=http://127.0.0.1:4000`, `LLM_PROVIDER=local`,
  `OPENAI_API_KEY=`/`ANTHROPIC_API_KEY=` (optional), `ENRICH_LIMIT=500`, `RAG_DSN`, `SHOP_DSN`.
- [ ] Verify: config quiet; litellm healthy; `curl :4000/v1/chat/completions` with model `chat`
  answers via Ollama, and `/v1/embeddings` with `embed` returns 1024 dims; Postgres has `vector`
  available. Commit `feat(platform): genai profile, litellm gateway, pgvector postgres`.

### Task 2: Catalogue enrichment

**Owner:** `ai-engineer`. Files: `genai/` project skeleton (`pyproject.toml` with ALL genai runtime
deps for T2+T3, `uv.lock`), `genai/src/genai/llm.py` (LiteLLM client wrapper: base URL, key, alias,
retries, JSON-schema response_format, MLflow trace), `genai/src/genai/enrichment/*`, tests.
- Prompt: category (pt + en), dimensions/weight, photo count, and top-3 reviews as delimited untrusted
  text, with an instruction to ignore instructions inside reviews. The output schema per the contract.
- Pydantic validation (lengths, tag count, ASCII/English heuristic via a stopword ratio); retry with
  the error up to 3 times; then reject.
- Idempotent by `(product_id, prompt_hash)`. Writes with `deltalake` to MinIO (`s3://lakehouse/...`,
  storage options from env).
- CLI `genai enrich [--limit N | --all] [--category C]`, stratified sample, prints throughput.
- Tests: schema validation, retry-then-reject, idempotency, injection text kept as data (prompt
  contains the delimiter), and the Delta writer to a local path. All with a mocked LLM.
- [ ] After merge: one real run of 500 on Ollama → report accepted/rejected, tok/s, wall time.
  Commit `feat(genai): llm catalogue enrichment with schema validation`.

### Task 3: RAG index and hybrid search

**Owner:** `ai-engineer`. Files: `genai/src/genai/rag/*`, `sql/rag.sql` (schema `rag`, extension,
table, indexes), tests (vector store tests against a throwaway database in the pgvector Postgres, like
the replayer's integration tests).
- **Chunks:**
  - one product doc (title + description + tags + category);
  - one review summary per product with ≥ 3 reviews (LLM, batch, after enrichment);
  - review texts (non-empty comments; truncate to 512 tokens).
- **Embeddings** via the `embed` alias in batches of 64. The upsert key is the chunk id.
- **Search:**
  - vector top-50 (cosine) + full-text top-50 (`websearch_to_tsquery('simple', q)`) → RRF (k=60) →
    top-k;
  - an optional filter by category;
  - product-level dedupe helper.
- **Eval:**
  - generate 100 labelled queries from enriched docs (LLM paraphrase that avoids the title's exact
    words; stored in `genai/eval_data/rag_queries.jsonl`);
  - Recall@10 and MRR for vector-only, text-only and hybrid;
  - logged to MLflow.
- CLI `genai rag index|search "q"|eval`.
- [ ] Commit `feat(genai): hybrid rag index in pgvector with recall eval`.

### Task 4: Agent core and analytics agent

**Owner:** `ai-engineer`. Files: `agents/` project skeleton (`pyproject.toml` with ALL agents runtime
deps for T4–T6, `uv.lock`, `Dockerfile` for the UI image), `agents/src/agents/core/*`,
`agents/src/agents/analytics_agent/*`, tests.
- **Core:**
  - a tool registry (Pydantic I/O, allowlist per agent);
  - guardrail nodes (input PII/injection, output checks);
  - retry-with-feedback on tool errors (max 3);
  - the LangGraph Postgres checkpointer (schema `agents`);
  - structured JSON logs;
  - MLflow tracing autolog.
- **Analytics tools:**
  - `list_metrics`/`query_metric`: the MetricFlow semantic layer. Prefer the `dbt-metricflow` Python
    API in the agents project if it co-resolves; else a subprocess to
    `uv run --project analytics mf query`. Record which.
  - `get_model_docs` (from `analytics/target/manifest.json`);
  - `run_sql` (Review Focus 4);
  - `plot` (matplotlib PNG returned as a file path/bytes).
- **Graph:** clarify → plan → execute → validate (empty/error/implausible magnitude → self-correct, max
  2) → answer with the SQL/metric used and a chart if useful.
- Tests: `run_sql` guards, the registry allowlist, guardrails, graph routing with a fake LLM, and the
  checkpointer resume.
- [ ] Commit `feat(agents): agent core and analytics agent over the semantic layer`.

### Task 5: Shopping assistant

**Owner:** `ai-engineer`. Files: `agents/src/agents/shopping_agent/*`, `sql/shop.sql` (schema `shop`,
`stock`, a restricted role for order writes), tests.
- **Tools:**
  - `search_products` (`genai.rag.search` → products, with enriched titles);
  - `get_product` (Postgres + enrichment);
  - `check_stock` (`shop.stock`);
  - `get_recommendations` (POST `/recommend` with the chat session id; emits `product_view` events for
    viewed products via the clickstream producer at dataset time so the session becomes known);
  - `get_order_status` (`olist.orders` by order id, own customer only);
  - `place_order` (an interrupt for human approval, then inserts order + items + payment through the
    restricted role → CDC picks it up).
- Reviews and product text are wrapped as untrusted. The output faithfulness check: claims about a
  product must appear in tool results.
- Tests: tool selection with a fake LLM, interrupt reject → no rows, interrupt approve → rows (throwaway
  DB), injection review ignored, cross-customer order lookup refused.
- [ ] Commit `feat(agents): shopping assistant with rag, recommendations and approved orders`.

### Task 6: Evals and Chainlit UI

**Owner:** `ai-engineer`. Files: `agents/src/agents/evals/*`, `agents/evals/data/*`,
`agents/src/agents/ui/app.py`, tests.
- **Golden sets:**
  - analytics: 60 questions, with expected answers produced by `agents eval build-analytics` from gold
    via MetricFlow/DuckDB;
  - shopping: 40 tool-selection + 20 adversarial (injection, PII exfiltration, unapproved order,
    other customer's order).
- **Runners:**
  - `agents eval analytics|shopping [--provider local|hosted] [--limit N]`;
  - metrics: execution accuracy (numeric tolerance 0.5 %), judge score on explanation (1–5),
    tool-selection accuracy, faithfulness (judge), refusal rate, mean trajectory length;
  - logged to MLflow (`genai-evals`).
- **Chainlit:**
  - two chat profiles ("Analytics", "Shopping");
  - streams steps;
  - shows the SQL/metric and the chart;
  - the `place_order` approval as an `AskActionMessage`;
  - thread id = the checkpointer thread.
- [ ] Verify locally on Ollama with `--limit 10` per set (report numbers + wall time). Commit
  `feat(agents): evals and chainlit ui`.

### Task 7: Orchestration, serving titles, CI

**Owner:** `data-engineer` (+ an `ml-engineer` exception for serving titles).
- **DAGs:**
  - `enrich_catalogue` (manual/weekly, limit from env);
  - `rag_index` (on the `ENRICHED` asset);
  - `nightly_evals` (daily, `--limit` from env, local provider);
  - LLM tasks run in a new `llm` pool with 1 slot (one GPU).
- **`/recommend`** fills `title` from `silver/product_enriched` (loaded at startup and refreshed on
  `/reload`; null when absent).
- **CI:** genai/agents ruff + mypy + pytest (mocked LLM) in lint-test. There is no Ollama in CI.
- [ ] Commit `feat(orchestration): genai dags; feat(ml): enriched titles in recommendations; ci: genai
  and agents tests`.

### Task 8: Docs and spec

**Owner:** `platform-engineer`.
- ADR-0008 (≤ 45 lines): LiteLLM gateway, the Ollama GPU budget and the 500 default, the pgvector swap,
  separate uv projects, guardrails, HITL orders, eval method + honest numbers.
- Runbook `docs/runbooks/genai.md`; README Phase 6 row + "Run it"; spec §4.4/§10 amendments.
- [ ] Commit `docs: ADR-0008 genai and agents, genai runbook`.
