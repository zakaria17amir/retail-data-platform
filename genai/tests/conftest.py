import hashlib
import json
import math
import random
from collections.abc import Sequence
from typing import Any

import pytest

from genai.enrichment.prompt import ProductContext
from genai.llm import Completion, Message

GOOD = {
    "title": "Solid Wood Double Bed Frame",
    "description": "A sturdy wooden bed frame that is easy to assemble and fits a standard "
    "double mattress, with a smooth finish for the bedroom.",
    "tags": ["bed", "bedroom", "wood"],
    "language": "en",
}
BAD = {**GOOD, "tags": ["bed"]}


class FakeLLM:
    def __init__(self, outputs: Sequence[dict[str, Any] | str]) -> None:
        self.outputs = list(outputs)
        self.calls: list[tuple[list[Message], str]] = []

    def __call__(self, messages: list[Message], prompt_hash: str) -> Completion:
        self.calls.append((list(messages), prompt_hash))
        out = self.outputs[min(len(self.calls), len(self.outputs)) - 1]
        text = out if isinstance(out, str) else json.dumps(out)
        return Completion(text=text, model="ollama/qwen2.5:7b-instruct", completion_tokens=50)


def context(product_id: str = "p1", category: str = "bed_bath_table") -> ProductContext:
    return ProductContext(
        product_id=product_id,
        category_pt="cama_mesa_banho",
        category_en=category,
        weight_g=1200.0,
        length_cm=40.0,
        height_cm=10.0,
        width_cm=30.0,
        photos=2,
        reviews=("Muito bom",),
    )


def fake_vector(text: str, dim: int = 1024) -> list[float]:
    """Deterministic unit vector: same text, same vector; unrelated texts are near-orthogonal."""
    rng = random.Random(int.from_bytes(hashlib.sha256(text.encode()).digest()[:8], "big"))
    values = [rng.gauss(0.0, 1.0) for _ in range(dim)]
    norm = math.sqrt(sum(v * v for v in values))
    return [v / norm for v in values]


class FakeEmbedder:
    def __init__(self) -> None:
        self.batches: list[list[str]] = []

    def __call__(self, texts: list[str]) -> list[list[float]]:
        self.batches.append(list(texts))
        return [fake_vector(t) for t in texts]


@pytest.fixture(autouse=True)
def _no_tracing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MLFLOW_TRACKING_URI", raising=False)
