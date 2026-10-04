"""`genai enrich [--limit N | --all] [--category C]`."""

from __future__ import annotations

import argparse
import os
from collections.abc import Sequence

from genai.enrichment.pipeline import LLM, run
from genai.enrichment.schema import json_schema
from genai.enrichment.sources import load_contexts, stratified
from genai.llm import Completion, LLMConfig, Message, complete_json, enable_tracing

DEFAULT_LIMIT = 500


def make_llm() -> LLM:
    config = LLMConfig.from_env()
    if config.trace:
        enable_tracing()
    schema = json_schema()

    def call(messages: list[Message], prompt_hash: str) -> Completion:
        return complete_json(
            config, messages, schema, "product_enrichment", tags={"prompt_hash": prompt_hash}
        )

    return call


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="genai")
    sub = p.add_subparsers(dest="command", required=True)
    enrich = sub.add_parser("enrich", help="LLM catalogue enrichment -> silver/product_enriched")
    size = enrich.add_mutually_exclusive_group()
    size.add_argument("--limit", type=int, help="stratified sample size (default ENRICH_LIMIT)")
    size.add_argument("--all", action="store_true", help="every current product")
    enrich.add_argument("--category", help="only this category (English or Portuguese name)")
    enrich.add_argument(
        "--root",
        default=f"s3://{os.environ.get('LAKEHOUSE_BUCKET') or 'lakehouse'}",
        help="lakehouse root (s3://bucket or a local path)",
    )
    enrich.add_argument("--batch-size", type=int, default=25, help="rows per Delta commit")
    return p


def resolve_limit(args: argparse.Namespace) -> int | None:
    if args.all:
        return None
    if args.limit is not None:
        return int(args.limit)
    return int(os.environ.get("ENRICH_LIMIT") or DEFAULT_LIMIT)


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
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
