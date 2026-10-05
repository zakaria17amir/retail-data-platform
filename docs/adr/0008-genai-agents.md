# ADR-0008: GenAI and agents — LiteLLM gateway, local Ollama, pgvector, guarded LangGraph agents

## Status
Accepted. Live pass on the RTX 4050 (qwen2.5:7b-instruct Q4 + bge-m3 via LiteLLM), numbers under "Measured".

## Context
Phase 6 adds enrichment, hybrid RAG and two LangGraph agents on one laptop: an RTX 4050 (6 GB) serves every call.

## Decision
- **Gateway:** `litellm` proxy (v1.101.0, digest-pinned). `chat`/`judge` → `ollama_chat/qwen2.5:7b-instruct`,
  `embed` → `ollama/bge-m3`, on host Ollama (`host.docker.internal:11434`). `chat-hosted` (openai/gpt-4.1-mini)
  and `judge-hosted` (anthropic/claude-haiku-4-5) are explicit: clients pick them only with `LLM_PROVIDER=hosted`
  and there are no router fallbacks, so nothing reaches a hosted model silently. Embeddings always stay local.
- **GPU budget:** Qwen2.5-7B Q4 (~4.7 GB) and bge-m3 (~1.2 GB) do not fit 6 GB together, so Ollama swaps them.
  `chat`/`judge` set `num_ctx: 8192` in `litellm/config.yaml`: Ollama's 2-4k default silently truncates from the
  start of the prompt (system prompt first). Qwen at 8k is 5.4 GB in `ollama ps`, split 24 %/76 % CPU/GPU.
  Batch jobs never interleave: `rag index` runs all review summaries (chat), then all embeddings (batches of 64).
  Default enrichment is **500 products**, stratified by category: measured 19.4 tok/s, 3.8 s/product, so
  500 ≈ 31 min and the spec's 5k ≈ 5 h.
- **pgvector:** Postgres → `pgvector/pgvector:0.8.6-pg16-bookworm` on the existing volume. Bookworm keeps the
  glibc 2.36 of `postgres:16.6`, so text-index collations stay valid (`-trixie` would not); 16.6 → 16.15 is a
  minor upgrade, same data format. RAG lives in schema `rag`, shop tables in `shop`, both outside `olist_cdc`.
- **Packaging:** `genai/` and `agents/` are separate uv projects (own locks, not workspace members); agents takes
  genai as an editable path dep. genai uses `mlflow-skinny` (`mlflow-tracing` lacks `log_metrics`), agents full
  `mlflow`, both 3.16.1. Both lock pandas 3.0.6 while `ml/` stays on 2.3.3; they never share an environment.
- **Semantic layer:** the MetricFlow 0.213.0 Python API runs in-process on `analytics/target/semantic_manifest.json`
  and its SQL runs on the same read-only gold DuckDB as `run_sql`. Rejected `dbt-metricflow`: its dbt-duckdb
  adapter opened the warehouse file read-write, loaded httpfs/delta and an S3 secret, and resolved paths from CWD.
- **`run_sql` guards:** sqlglot parse → exactly one query; no DDL/DML/COPY/ATTACH/PRAGMA/INSTALL, no `read_*`/file
  functions, unqualified gold tables only; LIMIT added (1000) or ≤ 1000; 10 s timeout; EXPLAIN (JSON) row estimate
  ≤ 50M. DuckDB views over `data/gold/*.parquet`, `read_only`, external access off, configuration locked.
- **Guardrails (`agents/core`):** input check for PII (CPF, email, BR phone, Luhn card) and EN/PT injection
  markers; per-agent tool allowlist; output check (no PII echo; numbers within 0.5 % of a tool result or the
  question, integers ≤ 10 exempt, else fallback to the tool data). Tool output is `<data>`-wrapped; reviews and
  snippets matching the injection markers are replaced by `[withheld: …]`.
- **Orders (HITL):** `place_order` is allowlisted only to principal `shopping_approver`. The `approve` node quotes
  the order and raises a LangGraph `interrupt`; only `{"approved": true}` writes, through role `shop_writer` (INSERT
  on `olist.orders`/`order_items`/`order_payments` only, `sql/shop.sql`). Orders stay `created`; no stock decrement.
- **Recommendations:** the agent emits no `product_view` events (needs `confluent-kafka[avro]` + the Avro schemas
  in agents), so `/recommend` never knows the chat session and serves cold-start popularity.
- **Evals:** analytics 60 golden cases with expected answers computed from gold through the agent's own tools
  (`agents eval build-analytics`); shopping 40 tool-selection + 20 adversarial; RAG 100 synthetic LLM-written queries
  labelled with their source product. The judge is the same local Qwen unless `--provider hosted` (self-preference
  bias).

## Measured (2026-10-05, local Qwen, `--limit 10` evals)
| Step | Result |
|---|---|
| Chat via LiteLLM | warm 142 tokens in 7.2 s (≈ 20 tok/s; Ollama decode 21.4 tok/s); first call 84 s (model load) |
| Enrichment 200 | 196 accepted, 4 rejected (all `description:not_english`, all terse English: check fixed), 753 s, 19.4 tok/s; re-run 200 skipped, Delta versions unchanged |
| RAG index | 196 products, 536 chunks (21 review summaries), 536 embedded in 144 s; re-run 0 summaries / 0 embedded / 0 pruned |
| RAG eval (100 queries) | Recall@10 / MRR: vector 0.99 / 0.895, text 0.02 / 0.02, hybrid 0.98 / 0.885 |
| Analytics eval | execution accuracy 0.70, judge 3.8/5, 0 errors, 11.4 s/case, wall 139 s (misses: filter, ratio, sql_marts) |
| Shopping eval (2 tool + 8 adversarial) | tool selection 1.0, refusal 0.75, attempted forbidden 0.125, false refusal 0, faithfulness 3.75, wall 125 s |
| HITL order | approve wrote 1 order/item/payment, total 65.06 = quote; reject wrote 0 rows |

## Consequences
- 7B tool calling works for single-metric questions; filters, ratios and raw SQL are where it fails. The text arm
  of hybrid search is near-useless on paraphrased queries (`websearch_to_tsquery` ANDs every term); an OR-ed query
  measured text 0.90 / 0.61 but hybrid MRR 0.78, so it is not adopted. The cost guard counts an ungrouped aggregate as
  1 row, so `query_metric` for `aov` without group-by passes (160.2412) and real cross products are still rejected.
  Rejected: Langfuse, router fallbacks.
