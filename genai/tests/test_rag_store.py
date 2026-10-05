"""pgvector tests on a throwaway database; skipped when `vector` is not available."""

import os
import uuid
from collections.abc import Iterator

import psycopg
import pytest
from conftest import FakeEmbedder, FakeLLM
from psycopg import sql
from psycopg.conninfo import make_conninfo

from genai.rag import dedupe_products, search
from genai.rag.chunks import Product, product_text
from genai.rag.eval import evaluate
from genai.rag.store import index, init, vector_available

SUMMARY = {"summary": "Customers say the bed is sturdy and that it arrived fast and well packed."}


@pytest.fixture(scope="module")
def dsn() -> Iterator[str]:
    base = os.environ.get("RAG_DSN") or os.environ.get("POSTGRES_DSN")
    if not base:
        pytest.skip("RAG_DSN / POSTGRES_DSN is not set")
    try:
        admin = psycopg.connect(base, autocommit=True, connect_timeout=3)
    except psycopg.OperationalError:
        pytest.skip("Postgres is unreachable")
    with admin:
        if not vector_available(admin):
            pytest.skip("the pgvector `vector` extension is not available on this Postgres")
        name = f"rag_test_{uuid.uuid4().hex[:8]}"
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        try:
            yield make_conninfo(base, dbname=name)
        finally:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))


@pytest.fixture
def conn(dsn: str) -> Iterator[psycopg.Connection[tuple[object, ...]]]:
    with psycopg.connect(dsn) as connection:
        init(connection)
        init(connection)
        yield connection
        connection.rollback()
        connection.execute("TRUNCATE rag.chunks")
        connection.commit()


BED = Product(
    "p1", "Wooden Bed Frame", "A sturdy double bed for the bedroom.", ("bed",), "furniture"
)
TOY = Product("p2", "Red Toy Car", "A small car for kids who love racing.", ("toy", "car"), "toys")
REVIEWS = {"p1": ["cama boa", "chegou rapido", "recomendo muito"], "p2": ["carrinho lindo"]}


def test_init_creates_table_generated_tsv_and_indexes(
    conn: psycopg.Connection[tuple[object, ...]],
) -> None:
    indexes = {
        r[0]
        for r in conn.execute(
            "SELECT indexdef FROM pg_indexes WHERE schemaname = 'rag' AND tablename = 'chunks'"
        )
    }
    assert any("USING hnsw (embedding vector_cosine_ops)" in i for i in indexes)
    assert any("USING gin (tsv)" in i for i in indexes)
    generated = conn.execute(
        "SELECT is_generated FROM information_schema.columns "
        "WHERE table_schema = 'rag' AND table_name = 'chunks' AND column_name = 'tsv'"
    ).fetchone()
    assert generated == ("ALWAYS",)


def test_index_is_idempotent_and_prunes_stale_docs(
    conn: psycopg.Connection[tuple[object, ...]],
) -> None:
    llm, embedder = FakeLLM([SUMMARY]), FakeEmbedder()
    stats = index(conn, [BED, TOY], REVIEWS, llm, embedder)
    counts = dict(
        conn.execute("SELECT source, count(*) FROM rag.chunks GROUP BY source").fetchall()
    )
    assert counts == {"product_doc": 2, "review": 4, "review_summary": 1}
    assert (stats.summaries, stats.embedded, stats.pruned) == (1, 7, 0)
    assert len(llm.calls) == 1

    again = index(conn, [BED, TOY], REVIEWS, llm, embedder)
    assert (again.summaries, again.embedded, again.pruned) == (0, 0, 0)
    assert len(llm.calls) == 1

    renamed = Product("p1", "Oak Bed Frame", BED.description, BED.tags, BED.category)
    changed = index(conn, [renamed, TOY], REVIEWS, llm, embedder)
    assert (changed.embedded, changed.pruned) == (1, 1)
    docs = conn.execute("SELECT text FROM rag.chunks WHERE source = 'product_doc'").fetchall()
    assert sorted(docs) == sorted([(product_text(renamed),), (product_text(TOY),)])


def test_search_hybrid_text_vector_category_and_dedupe(
    conn: psycopg.Connection[tuple[object, ...]],
) -> None:
    embedder = FakeEmbedder()
    index(conn, [BED, TOY], REVIEWS, FakeLLM([SUMMARY]), embedder)

    exact = search(product_text(TOY), k=3, mode="vector", conn=conn, embedder=embedder)
    assert (exact[0].product_id, exact[0].source, exact[0].rank_vector) == ("p2", "product_doc", 1)
    assert len(search("anything", k=50, mode="vector", conn=conn, embedder=embedder)) == 7

    text = search("racing car", mode="text", conn=conn, embedder=embedder)
    assert [h.product_id for h in text] == ["p2"]
    assert (text[0].rank_text, text[0].rank_vector) == (1, None)

    hybrid = search("sturdy bed", conn=conn, embedder=embedder)
    assert hybrid[0].product_id == "p1" and hybrid[0].rank_text == 1
    assert hybrid[0].score > hybrid[-1].score
    assert {h.product_id for h in dedupe_products(hybrid)} == {"p1", "p2"}

    toys = search("sturdy bed", category="toys", conn=conn, embedder=embedder)
    assert toys and {h.product_id for h in toys} == {"p2"}


def test_evaluate_reports_recall_and_mrr_per_mode(
    conn: psycopg.Connection[tuple[object, ...]],
) -> None:
    embedder = FakeEmbedder()
    index(conn, [BED, TOY], REVIEWS, FakeLLM([SUMMARY]), embedder)
    queries = [
        {"query": product_text(TOY), "product_id": "p2"},
        {"query": "sturdy bedroom", "product_id": "p1"},
    ]
    metrics = evaluate(conn, queries, embedder)
    assert set(metrics) == {
        f"{m}_{k}" for m in ("vector", "text", "hybrid") for k in ("recall_at_10", "mrr")
    }
    assert metrics["hybrid_recall_at_10"] == 1.0
    assert metrics["text_mrr"] == 1.0
