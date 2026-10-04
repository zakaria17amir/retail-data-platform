"""`genai enrich [--limit N | --all] [--category C]` and `genai rag init|index|search "q"|eval`."""

from __future__ import annotations

import argparse
import hashlib
import os
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from genai.enrichment.pipeline import LLM, run
from genai.enrichment.schema import json_schema
from genai.enrichment.sources import load_contexts, stratified
from genai.llm import Completion, LLMConfig, Message, complete_json, enable_tracing
from genai.rag import search
from genai.rag.chunks import ReviewSummary, load_products, load_reviews
from genai.rag.embed import EMBED_ALIAS, litellm_embedder
from genai.rag.eval import (
    N_QUERIES,
    QUERIES_PATH,
    GeneratedQuery,
    evaluate,
    generate_queries,
    log_to_mlflow,
    read_queries,
    write_queries,
)
from genai.rag.search import CANDIDATES, MODES, RRF_K
from genai.rag.store import connect, index, init

DEFAULT_LIMIT = 500


def make_llm(name: str = "product_enrichment", schema: dict[str, Any] | None = None) -> LLM:
    config = LLMConfig.from_env()
    if config.trace:
        enable_tracing()
    schema = schema or json_schema()

    def call(messages: list[Message], prompt_hash: str) -> Completion:
        return complete_json(config, messages, schema, name, tags={"prompt_hash": prompt_hash})

    return call


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="genai")
    sub = p.add_subparsers(dest="command", required=True)
    root = argparse.ArgumentParser(add_help=False)
    root.add_argument(
        "--root",
        default=f"s3://{os.environ.get('LAKEHOUSE_BUCKET') or 'lakehouse'}",
        help="lakehouse root (s3://bucket or a local path)",
    )
    enrich = sub.add_parser(
        "enrich", parents=[root], help="LLM catalogue enrichment -> silver/product_enriched"
    )
    size = enrich.add_mutually_exclusive_group()
    size.add_argument("--limit", type=int, help="stratified sample size (default ENRICH_LIMIT)")
    size.add_argument("--all", action="store_true", help="every current product")
    enrich.add_argument("--category", help="only this category (English or Portuguese name)")
    enrich.add_argument("--batch-size", type=int, default=25, help="rows per Delta commit")

    rag = sub.add_parser("rag", help="hybrid RAG index in pgvector (RAG_DSN)").add_subparsers(
        dest="rag_command", required=True
    )
    rag.add_parser("init", help="apply sql/rag.sql (extension, schema, table, indexes)")
    rag.add_parser("index", parents=[root], help="chunk, summarise, embed and upsert")
    find = rag.add_parser("search", help="hybrid search")
    find.add_argument("query")
    find.add_argument("--k", type=int, default=10)
    find.add_argument("--category", help="English category name")
    find.add_argument("--mode", choices=MODES, default="hybrid")
    ev = rag.add_parser("eval", parents=[root], help="Recall@10 and MRR per retrieval mode")
    ev.add_argument("--queries", type=Path, default=QUERIES_PATH, help="generated if missing")
    ev.add_argument("--n", type=int, default=N_QUERIES, help="queries to generate")
    return p


def resolve_limit(args: argparse.Namespace) -> int | None:
    if args.all:
        return None
    if args.limit is not None:
        return int(args.limit)
    return int(os.environ.get("ENRICH_LIMIT") or DEFAULT_LIMIT)


def _print_rejects(kind: str, rejects: Sequence[tuple[str, Any]]) -> None:
    for pid, r in rejects:
        print(f"{kind} reject {pid} {r.rule_id}: {r.reason}")


def rag_main(args: argparse.Namespace) -> int:
    if args.rag_command == "search":
        hits = search(args.query, args.k, category=args.category, mode=args.mode)
        for i, h in enumerate(hits, 1):
            text = " ".join(h.text.split())[:120]
            ranks = f"v={h.rank_vector or '-'} t={h.rank_text or '-'}"
            print(f"{i}  {h.score:.4f}  {h.product_id}  {h.source}  {ranks}  {text}")
        return 0
    with connect() as conn:
        if args.rag_command == "init":
            init(conn)
            print("rag.chunks ready")
            return 0
        embedder = litellm_embedder(LLMConfig.from_env())
        if args.rag_command == "index":
            products = load_products(args.root)
            reviews = load_reviews(args.root, {p.product_id for p in products})
            llm = make_llm("review_summary", ReviewSummary.model_json_schema())
            stats = index(conn, products, reviews, llm, embedder)
            _print_rejects("summary", stats.rejects)
            print(
                f"{stats.products} products, {stats.chunks} chunks, {stats.summaries} summaries, "
                f"{len(stats.rejects)} rejected, {stats.embedded} embedded, {stats.pruned} pruned "
                f"in {stats.seconds:.1f} s"
            )
            return 0
        if not args.queries.exists():
            llm = make_llm("rag_query", GeneratedQuery.model_json_schema())
            rows, rejects = generate_queries(load_products(args.root), llm, args.n)
            _print_rejects("query", rejects)
            write_queries(args.queries, rows)
            print(f"wrote {len(rows)} queries to {args.queries}")
        queries = read_queries(args.queries)
        metrics = evaluate(conn, queries, embedder)
    for name, value in metrics.items():
        print(f"{name}: {value:.4f}")
    if os.environ.get("MLFLOW_TRACKING_URI"):
        params = {
            "n_queries": len(queries),
            "queries_sha": hashlib.sha256(args.queries.read_bytes()).hexdigest()[:12],
            "candidates": CANDIDATES,
            "rrf_k": RRF_K,
            "embed_alias": EMBED_ALIAS,
            "chat_alias": LLMConfig.from_env().model,
        }
        print(f"mlflow run {log_to_mlflow(metrics, params)}")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.command == "rag":
        return rag_main(args)
    contexts = stratified(load_contexts(args.root), resolve_limit(args), args.category)
    stats = run(args.root, contexts, make_llm(), args.batch_size)
    done = stats.accepted + stats.rejected
    seconds = max(stats.seconds, 1e-9)
    print(
        f"{stats.accepted} accepted, {stats.rejected} rejected, {stats.skipped} skipped "
        f"in {stats.seconds:.1f} s: {done / seconds:.3f} products/s, "
        f"{stats.completion_tokens / seconds:.1f} tok/s"
    )
    return 0
