import json
from pathlib import Path

from conftest import BAD, GOOD, FakeLLM, context
from deltalake import DeltaTable

from genai.enrichment.pipeline import Accepted, Rejected, enrich_one, run
from genai.enrichment.writer import ENRICHED, REJECTS


def test_first_valid_output_is_accepted() -> None:
    llm = FakeLLM([GOOD])
    result = enrich_one(context(), llm)
    assert isinstance(result, Accepted)
    assert result.enrichment.title == GOOD["title"]
    assert result.model == "ollama/qwen2.5:7b-instruct"
    assert len(llm.calls) == 1


def test_retry_feeds_back_the_error_then_accepts() -> None:
    llm = FakeLLM([BAD, GOOD])
    result = enrich_one(context(), llm)
    assert isinstance(result, Accepted)
    retry_messages, _ = llm.calls[1]
    assert retry_messages[-2] == {"role": "assistant", "content": json.dumps(BAD)}
    assert retry_messages[-1]["role"] == "user"
    assert "tags" in retry_messages[-1]["content"]


def test_three_invalid_outputs_are_rejected_with_reason() -> None:
    llm = FakeLLM([BAD, "not json", BAD])
    result = enrich_one(context(), llm)
    assert isinstance(result, Rejected)
    assert len(llm.calls) == 3
    assert result.rule_id == "tags:too_short"
    assert "at least 3" in result.reason
    assert result.raw == json.dumps(BAD)
    assert {h for _, h in llm.calls} == {result.prompt_hash}


def test_run_writes_accepted_and_rejected_and_is_idempotent(tmp_path: Path) -> None:
    root = str(tmp_path)
    contexts = [context("p1"), context("p2")]
    llm = FakeLLM([GOOD, BAD, BAD, BAD])
    first = run(root, contexts, llm, batch_size=1)
    assert (first.accepted, first.rejected) == (1, 1)

    enriched = DeltaTable(f"{root}/{ENRICHED}").to_pyarrow_table().to_pylist()
    rejects = DeltaTable(f"{root}/{REJECTS}").to_pyarrow_table().to_pylist()
    assert [r["product_id"] for r in enriched] == ["p1"]
    assert enriched[0]["tags"] == GOOD["tags"]
    assert enriched[0]["language"] == "en"
    assert [(r["product_id"], r["rule_id"]) for r in rejects] == [("p2", "tags:too_short")]

    rerun_llm = FakeLLM([GOOD])
    second = run(root, contexts, rerun_llm, batch_size=1)
    assert (second.accepted, second.rejected, second.skipped) == (0, 0, 2)
    assert rerun_llm.calls == []
    assert DeltaTable(f"{root}/{ENRICHED}").version() == 0
    assert DeltaTable(f"{root}/{REJECTS}").version() == 0


def test_changed_inputs_change_the_hash_and_reenrich(tmp_path: Path) -> None:
    root = str(tmp_path)
    run(root, [context("p1")], FakeLLM([GOOD]), batch_size=10)
    changed = context("p1", category="furniture_bedroom")
    stats = run(root, [changed], FakeLLM([GOOD]), batch_size=10)
    assert stats.accepted == 1
