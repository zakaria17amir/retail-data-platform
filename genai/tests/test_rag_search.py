import pytest

from genai.rag.search import Hit, dedupe_products, fuse, rank, text_sql, vector_sql


def row(cid: str, pid: str = "p") -> tuple[str, str, str, str]:
    return (cid, pid, "review", f"text {cid}")


def test_fuse_is_reciprocal_rank_fusion_with_k_60() -> None:
    hits = fuse([row("a"), row("b")], [row("b"), row("c")])
    assert [h.chunk_id for h in hits] == ["b", "a", "c"]
    b, a, c = hits
    assert b.score == pytest.approx(1 / 62 + 1 / 61)
    assert (b.rank_vector, b.rank_text) == (2, 1)
    assert (a.score, a.rank_vector, a.rank_text) == (pytest.approx(1 / 61), 1, None)
    assert (c.rank_vector, c.rank_text) == (None, 2)
    assert (b.product_id, b.source, b.text) == ("p", "review", "text b")


def test_fuse_breaks_score_ties_by_chunk_id() -> None:
    assert [h.chunk_id for h in fuse([row("z")], [row("y")])] == ["y", "z"]


def test_rank_modes_use_one_or_both_lists() -> None:
    vector, text = [row("a")], [row("b")]
    assert [h.chunk_id for h in rank(vector, text, "vector")] == ["a"]
    assert [h.chunk_id for h in rank(vector, text, "text")] == ["b"]
    assert {h.chunk_id for h in rank(vector, text, "hybrid")} == {"a", "b"}


def test_dedupe_products_keeps_the_best_hit_per_product() -> None:
    hits = [
        Hit("p1", "a", "review", "", 0.3, 1, None),
        Hit("p2", "b", "product_doc", "", 0.2, 2, None),
        Hit("p1", "c", "product_doc", "", 0.1, 3, None),
    ]
    assert [h.chunk_id for h in dedupe_products(hits)] == ["a", "b"]


def test_vector_sql_orders_by_cosine_distance_with_optional_category() -> None:
    sql = vector_sql(category=False)
    assert "ORDER BY embedding <=> %(embedding)s::vector LIMIT %(limit)s" in sql
    assert "category" not in sql
    assert "WHERE category = %(category)s" in vector_sql(category=True)


def test_text_sql_uses_websearch_tsquery_with_simple_config() -> None:
    sql = text_sql(category=False)
    assert "websearch_to_tsquery('simple', %(query)s)" in sql
    assert "tsv @@ q" in sql
    assert "ORDER BY ts_rank_cd(tsv, q) DESC" in sql
    assert "category" not in sql
    assert "AND category = %(category)s" in text_sql(category=True)
