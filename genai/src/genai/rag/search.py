"""Hybrid retrieval: vector top-50 (cosine) + full-text top-50, fused by reciprocal rank (k=60)."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Literal

import psycopg

from genai.llm import LLMConfig
from genai.rag.embed import Embedder, litellm_embedder
from genai.rag.store import connect, vector_literal

CANDIDATES = 50
RRF_K = 60
EF_SEARCH = 2 * CANDIDATES
Mode = Literal["vector", "text", "hybrid"]
MODES: tuple[Mode, ...] = ("vector", "text", "hybrid")
Row = tuple[str, str, str, str]  # chunk_id, product_id, source, text
COLUMNS = "chunk_id, product_id, source, text"


@dataclass(frozen=True)
class Hit:
    product_id: str
    chunk_id: str
    source: str
    text: str
    score: float
    rank_vector: int | None
    rank_text: int | None


def vector_sql(category: bool) -> str:
    where = "WHERE category = %(category)s " if category else ""
    return (
        f"SELECT {COLUMNS} FROM rag.chunks {where}"
        "ORDER BY embedding <=> %(embedding)s::vector LIMIT %(limit)s"
    )


def text_sql(category: bool) -> str:
    extra = " AND category = %(category)s" if category else ""
    return (
        f"SELECT {COLUMNS} FROM rag.chunks, websearch_to_tsquery('simple', %(query)s) AS q "
        f"WHERE tsv @@ q{extra} ORDER BY ts_rank_cd(tsv, q) DESC, chunk_id LIMIT %(limit)s"
    )


def fuse(vector: Sequence[Row], text: Sequence[Row], k: int = RRF_K) -> list[Hit]:
    rows: dict[str, Row] = {}
    ranks: dict[str, dict[str, int]] = {}
    for name, ranked in (("vector", vector), ("text", text)):
        for rank_, row in enumerate(ranked, 1):
            rows.setdefault(row[0], row)
            ranks.setdefault(row[0], {})[name] = rank_
    hits = [
        Hit(
            product_id=row[1],
            chunk_id=cid,
            source=row[2],
            text=row[3],
            score=sum(1 / (k + r) for r in ranks[cid].values()),
            rank_vector=ranks[cid].get("vector"),
            rank_text=ranks[cid].get("text"),
        )
        for cid, row in rows.items()
    ]
    return sorted(hits, key=lambda h: (-h.score, h.chunk_id))


def rank(vector: Sequence[Row], text: Sequence[Row], mode: Mode) -> list[Hit]:
    return fuse(vector if mode != "text" else [], text if mode != "vector" else [])


def dedupe_products(hits: Sequence[Hit]) -> list[Hit]:
    best: dict[str, Hit] = {}
    for h in hits:
        best.setdefault(h.product_id, h)
    return list(best.values())


def retrieve(
    conn: psycopg.Connection[Any],
    query: str,
    embedding: Sequence[float],
    category: str | None = None,
) -> tuple[list[Row], list[Row]]:
    params = {
        "query": query,
        "embedding": vector_literal(embedding),
        "category": category,
        "limit": CANDIDATES,
    }
    with conn.transaction():
        # ef_search >= LIMIT, and iterative scans keep filtered HNSW results from coming up short
        conn.execute(f"SET LOCAL hnsw.ef_search = {EF_SEARCH}")
        conn.execute("SET LOCAL hnsw.iterative_scan = strict_order")
        vector = conn.execute(vector_sql(category is not None), params).fetchall()
        text = conn.execute(text_sql(category is not None), params).fetchall()
    return vector, text


def search(
    query: str,
    k: int = 10,
    *,
    category: str | None = None,
    mode: Mode = "hybrid",
    conn: psycopg.Connection[Any] | None = None,
    embedder: Embedder | None = None,
) -> list[Hit]:
    embed = embedder or litellm_embedder(LLMConfig.from_env())
    (embedding,) = embed([query])
    if conn is None:
        with connect() as own:
            vector, text = retrieve(own, query, embedding, category)
    else:
        vector, text = retrieve(conn, query, embedding, category)
    return rank(vector, text, mode)[:k]
