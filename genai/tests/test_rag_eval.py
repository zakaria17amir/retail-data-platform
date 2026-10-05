from pathlib import Path

import mlflow
import pytest
from conftest import FakeLLM

from genai.rag.chunks import Product
from genai.rag.eval import (
    QUERIES_PATH,
    generate_queries,
    log_to_mlflow,
    mrr,
    overlap,
    query_messages,
    read_queries,
    recall_at_k,
    write_queries,
)


def prod(pid: str, title: str = "Wooden Bed Frame") -> Product:
    return Product(pid, title, "A sturdy frame for the bedroom.", ("bed", "wood"), "furniture")


def test_recall_and_mrr_over_product_rankings() -> None:
    ranked = [["a", "b"], ["c", "x"], ["y", "z"]]
    labels = ["a", "x", "q"]
    assert recall_at_k(ranked, labels, k=10) == pytest.approx(2 / 3)
    assert recall_at_k(ranked, labels, k=1) == pytest.approx(1 / 3)
    assert mrr(ranked, labels, k=10) == pytest.approx((1 + 1 / 2 + 0) / 3)
    assert mrr(ranked, labels, k=1) == pytest.approx(1 / 3)
    assert recall_at_k([], [], k=10) == mrr([], [], k=10) == 0.0


def test_overlap_finds_content_words_shared_with_the_title() -> None:
    assert overlap("a sturdy wood bed for my room", "Wooden Bed Frame") == {"bed"}
    assert overlap("somewhere to sleep, timber made", "Wooden Bed Frame") == set()
    assert overlap("for the kids", "The Toy for Kids") == {"kids"}


def test_query_prompt_lists_the_forbidden_title_words_and_delimits_the_doc() -> None:
    system, user = query_messages(prod("p1"))
    assert "do not use" in system["content"].lower()
    assert "<<<PRODUCT>>>" in user["content"]
    assert "Forbidden words: bed, frame, wooden" in user["content"]


def test_generate_queries_retries_on_title_words_and_labels_the_product() -> None:
    llm = FakeLLM([{"query": "wooden bed please"}, {"query": "somewhere sturdy to sleep"}])
    rows, rejects = generate_queries([prod("p1")], llm, n=1)
    assert rows == [{"query": "somewhere sturdy to sleep", "product_id": "p1"}]
    assert rejects == []
    assert "repeats title words: bed, wooden" in llm.calls[1][0][-1]["content"]


def test_generate_queries_skips_rejects_until_n_and_is_deterministic() -> None:
    products = [prod(f"p{i}") for i in range(5)]
    llm = FakeLLM([{"query": "bed"}])
    rows, rejects = generate_queries(products, llm, n=2)
    assert rows == [] and len(rejects) == 5
    assert rejects[0][1].rule_id == "check"
    ok = FakeLLM([{"query": "timber sleeping place"}])
    first, _ = generate_queries(products, ok, n=2)
    again, _ = generate_queries(list(reversed(products)), ok, n=2)
    assert len(first) == 2 and first == again


def test_queries_round_trip_as_jsonl(tmp_path: Path) -> None:
    path = tmp_path / "q" / "rag_queries.jsonl"
    rows = [{"query": "somewhere to sleep", "product_id": "p1"}]
    write_queries(path, rows)
    assert read_queries(path) == rows
    assert QUERIES_PATH.parts[-3:] == ("genai", "eval_data", "rag_queries.jsonl")


def test_log_to_mlflow_records_metrics_and_params(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MLFLOW_ALLOW_FILE_STORE", "true")
    mlflow.set_tracking_uri(tmp_path.as_uri())
    try:
        run_id = log_to_mlflow({"hybrid_recall_at_10": 0.5}, {"n_queries": 100})
        run = mlflow.get_run(run_id)
    finally:
        mlflow.set_tracking_uri(None)  # type: ignore[arg-type]
    assert run.data.metrics == {"hybrid_recall_at_10": 0.5}
    assert run.data.params == {"n_queries": "100"}
    assert run.info.run_name == "rag-eval"
