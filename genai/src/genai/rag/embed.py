"""Embeddings through the LiteLLM `embed` alias (bge-m3, 1024 dims), in batches of 64."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from itertools import batched
from typing import Any

import litellm

from genai.llm import LLMConfig

BATCH_SIZE = 64
EMBED_ALIAS = "embed"
Embedder = Callable[[list[str]], list[list[float]]]


def litellm_embedder(config: LLMConfig) -> Embedder:
    """Always the local `embed` alias: LLM_PROVIDER=hosted only switches chat models."""

    def embed(texts: list[str]) -> list[list[float]]:
        response: Any = litellm.embedding(
            model=f"litellm_proxy/{EMBED_ALIAS}",
            api_base=config.base_url,
            api_key=config.api_key,
            input=texts,
            num_retries=config.num_retries,
            timeout=config.timeout,
        )
        data = sorted(response.data, key=lambda d: d["index"])
        return [list(d["embedding"]) for d in data]

    return embed


def embed_batches[T](
    items: Sequence[T], embedder: Embedder, text: Callable[[T], str] = str
) -> Iterator[tuple[tuple[T, ...], list[list[float]]]]:
    for batch in batched(items, BATCH_SIZE):
        yield batch, embedder([text(item) for item in batch])
