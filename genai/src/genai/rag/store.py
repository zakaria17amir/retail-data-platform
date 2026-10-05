"""`rag.chunks` in pgvector: schema init, idempotent indexing (summaries, then embeddings)."""

from __future__ import annotations

import os
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg

from genai.enrichment.pipeline import LLM
from genai.rag.chunks import MIN_REVIEWS, Chunk, Product, Reject, doc_chunks, summarize
from genai.rag.embed import Embedder, embed_batches

SQL_PATH = Path(__file__).resolve().parents[4] / "sql" / "rag.sql"
DEFAULT_DSN = "postgresql://retail:retail@127.0.0.1:5432/retail"
Conn = psycopg.Connection[Any]


@dataclass(frozen=True)
class IndexStats:
    products: int
    chunks: int
    summaries: int
    rejects: list[tuple[str, Reject]]
    embedded: int
    pruned: int
    seconds: float


def connect(env: Mapping[str, str] = os.environ) -> Conn:
    return psycopg.connect(env.get("RAG_DSN") or DEFAULT_DSN)


def vector_available(conn: Conn) -> bool:
    found = conn.execute("SELECT 1 FROM pg_available_extensions WHERE name = 'vector'").fetchone()
    return found is not None


def init(conn: Conn) -> None:
    conn.execute(SQL_PATH.read_text(encoding="utf-8").encode())
    conn.commit()


def vector_literal(vector: Sequence[float]) -> str:
    return "[" + ",".join(repr(float(v)) for v in vector) + "]"


def _insert(conn: Conn, chunks: Sequence[Chunk], vectors: Sequence[Sequence[float]]) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO rag.chunks (chunk_id, source, product_id, category, text, embedding) "
            "VALUES (%s, %s, %s, %s, %s, %s::vector) ON CONFLICT (chunk_id) DO NOTHING",
            [
                (c.chunk_id, c.source, c.product_id, c.category, c.text, vector_literal(v))
                for c, v in zip(chunks, vectors, strict=True)
            ],
        )
    conn.commit()


def index(
    conn: Conn,
    products: Sequence[Product],
    reviews: Mapping[str, Sequence[str]],
    llm: LLM,
    embedder: Embedder,
) -> IndexStats:
    """All chat calls (summaries) run before all embed calls, so Ollama swaps models once.

    A product's summary is generated once; delete its `review_summary` row to refresh it.
    """
    start = time.perf_counter()
    ids = [p.product_id for p in products]
    summarized = {
        r[0]
        for r in conn.execute(
            "SELECT product_id FROM rag.chunks "
            "WHERE source = 'review_summary' AND product_id = ANY(%s)",
            (ids,),
        )
    }
    chunks = doc_chunks(products, reviews)
    rejects: list[tuple[str, Reject]] = []
    summaries = 0
    for p in products:
        texts = reviews.get(p.product_id, ())
        if len(texts) < MIN_REVIEWS or p.product_id in summarized:
            continue
        out = summarize(p, texts, llm)
        if isinstance(out, Reject):
            rejects.append((p.product_id, out))
        else:
            chunks.append(out)
            summaries += 1
    keep = [c.chunk_id for c in chunks]
    existing = {
        r[0]
        for r in conn.execute("SELECT chunk_id FROM rag.chunks WHERE chunk_id = ANY(%s)", (keep,))
    }
    new = [c for c in chunks if c.chunk_id not in existing]
    for batch, vectors in embed_batches(new, embedder, lambda c: c.text):
        _insert(conn, batch, vectors)
    pruned = conn.execute(
        "DELETE FROM rag.chunks WHERE product_id = ANY(%s) AND source <> 'review_summary' "
        "AND NOT chunk_id = ANY(%s)",
        (ids, keep),
    ).rowcount
    conn.commit()
    seconds = time.perf_counter() - start
    return IndexStats(len(products), len(chunks), summaries, rejects, len(new), pruned, seconds)
