"""Enrich products with retry-with-feedback validation; idempotent by (product_id, prompt_hash)."""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from pydantic import ValidationError

from genai.enrichment.prompt import ProductContext, build_messages, prompt_hash
from genai.enrichment.schema import Enrichment, describe_error
from genai.enrichment.writer import (
    ENRICHED,
    ENRICHED_SCHEMA,
    REJECTS,
    REJECTS_SCHEMA,
    append,
    done_keys,
)
from genai.llm import Completion, Message

MAX_ATTEMPTS = 3
LLM = Callable[[list[Message], str], Completion]


@dataclass(frozen=True)
class Accepted:
    product_id: str
    prompt_hash: str
    model: str
    enrichment: Enrichment
    completion_tokens: int


@dataclass(frozen=True)
class Rejected:
    product_id: str
    prompt_hash: str
    model: str
    rule_id: str
    reason: str
    raw: str
    completion_tokens: int


@dataclass(frozen=True)
class Stats:
    accepted: int
    rejected: int
    skipped: int
    completion_tokens: int
    seconds: float


def enrich_one(ctx: ProductContext, llm: LLM) -> Accepted | Rejected:
    messages = build_messages(ctx)
    key = prompt_hash(messages)
    tokens = 0
    for _ in range(MAX_ATTEMPTS):
        out = llm(messages, key)
        tokens += out.completion_tokens
        try:
            enrichment = Enrichment.model_validate_json(out.text)
        except ValidationError as err:
            rule_id, reason = describe_error(err)
            messages = [
                *messages,
                {"role": "assistant", "content": out.text},
                {"role": "user", "content": f"Invalid output: {reason}. Return corrected JSON."},
            ]
            continue
        return Accepted(ctx.product_id, key, out.model, enrichment, tokens)
    return Rejected(ctx.product_id, key, out.model, rule_id, reason, out.text, tokens)


def _rows(
    results: Sequence[Accepted | Rejected],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    now = datetime.now(UTC)
    accepted, rejected = [], []
    for r in results:
        base = {"product_id": r.product_id, "prompt_hash": r.prompt_hash, "model": r.model}
        if isinstance(r, Accepted):
            accepted.append({**base, **r.enrichment.model_dump(), "enriched_at": now})
        else:
            rejected.append(
                {
                    **base,
                    "rule_id": r.rule_id,
                    "reason": r.reason,
                    "record_json": r.raw,
                    "rejected_at": now,
                }
            )
    return accepted, rejected


def run(root: str, contexts: Sequence[ProductContext], llm: LLM, batch_size: int = 25) -> Stats:
    done = done_keys(root)
    todo = [c for c in contexts if (c.product_id, prompt_hash(build_messages(c))) not in done]
    start = time.perf_counter()
    counts = {"accepted": 0, "rejected": 0, "tokens": 0}
    batch: list[Accepted | Rejected] = []
    for i, ctx in enumerate(todo, 1):
        result = enrich_one(ctx, llm)
        batch.append(result)
        counts["accepted" if isinstance(result, Accepted) else "rejected"] += 1
        counts["tokens"] += result.completion_tokens
        if len(batch) >= batch_size or i == len(todo):
            accepted, rejected = _rows(batch)
            append(root, ENRICHED, accepted, ENRICHED_SCHEMA)
            append(root, REJECTS, rejected, REJECTS_SCHEMA)
            batch = []
    return Stats(
        counts["accepted"],
        counts["rejected"],
        len(contexts) - len(todo),
        counts["tokens"],
        time.perf_counter() - start,
    )
