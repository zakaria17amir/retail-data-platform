import math
from types import SimpleNamespace
from typing import Any

import litellm
import pytest
from conftest import FakeEmbedder, fake_vector

from genai.llm import LLMConfig
from genai.rag.embed import BATCH_SIZE, embed_batches, litellm_embedder


def test_fake_vector_is_a_deterministic_1024_dim_unit_vector() -> None:
    v = fake_vector("bed")
    assert len(v) == 1024
    assert math.isclose(sum(x * x for x in v), 1.0)
    assert v == fake_vector("bed") != fake_vector("toy")


def test_embed_batches_of_64_in_order() -> None:
    embedder = FakeEmbedder()
    texts = [f"t{i}" for i in range(150)]
    out = list(embed_batches(texts, embedder))
    assert BATCH_SIZE == 64
    assert [len(b) for b in embedder.batches] == [64, 64, 22]
    assert [t for batch, _ in out for t in batch] == texts
    assert out[0][1][0] == fake_vector("t0")


def test_litellm_embedder_calls_the_embed_alias_even_in_hosted_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, Any] = {}

    def fake(**kwargs: Any) -> Any:
        seen.update(kwargs)
        data = [{"index": i, "embedding": [float(i)]} for i in range(len(kwargs["input"]))]
        return SimpleNamespace(data=list(reversed(data)))

    monkeypatch.setattr(litellm, "embedding", fake)
    config = LLMConfig.from_env(
        {"LITELLM_URL": "http://proxy:4000", "LITELLM_MASTER_KEY": "k", "LLM_PROVIDER": "hosted"}
    )
    assert litellm_embedder(config)(["a", "b"]) == [[0.0], [1.0]]
    assert seen["model"] == "litellm_proxy/embed"
    assert (seen["api_base"], seen["api_key"], seen["input"]) == (
        "http://proxy:4000",
        "k",
        ["a", "b"],
    )
