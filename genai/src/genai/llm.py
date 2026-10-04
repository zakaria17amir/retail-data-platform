"""LiteLLM SDK client for the LiteLLM proxy: one alias, JSON-schema output, MLflow tracing."""

from __future__ import annotations

import os
from collections.abc import Mapping
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any

import litellm
import mlflow

Message = dict[str, str]
litellm.suppress_debug_info = True


@dataclass(frozen=True)
class LLMConfig:
    base_url: str = "http://127.0.0.1:4000"
    api_key: str = ""
    model: str = "chat"
    num_retries: int = 2
    timeout: float = 300.0
    trace: bool = False

    @classmethod
    def from_env(cls, env: Mapping[str, str] = os.environ) -> LLMConfig:
        return cls(
            base_url=env.get("LITELLM_URL") or cls.base_url,
            api_key=env.get("LITELLM_MASTER_KEY", ""),
            model="chat-hosted" if env.get("LLM_PROVIDER") == "hosted" else cls.model,
            trace=bool(env.get("MLFLOW_TRACKING_URI")),
        )


@dataclass(frozen=True)
class Completion:
    text: str
    model: str
    completion_tokens: int


def enable_tracing(experiment: str = "genai") -> None:
    mlflow.set_experiment(experiment)
    mlflow.litellm.autolog()


def complete_json(
    config: LLMConfig,
    messages: list[Message],
    schema: dict[str, Any],
    name: str,
    tags: Mapping[str, str] | None = None,
    **kwargs: Any,
) -> Completion:
    """`kwargs` pass through to `litellm.completion` (e.g. `mock_response` in tests)."""
    span: Any = mlflow.start_span(name=name) if config.trace else nullcontext()
    with span:
        if config.trace:
            mlflow.update_current_trace(tags={"model_alias": config.model, **(tags or {})})
        response: Any = litellm.completion(
            model=f"litellm_proxy/{config.model}",
            api_base=config.base_url,
            api_key=config.api_key,
            messages=messages,
            response_format={
                "type": "json_schema",
                "json_schema": {"name": name, "schema": schema, "strict": True},
            },
            num_retries=config.num_retries,
            timeout=config.timeout,
            **kwargs,
        )
    usage = getattr(response, "usage", None)
    return Completion(
        text=response.choices[0].message.content or "",
        model=response.model or config.model,
        completion_tokens=int(getattr(usage, "completion_tokens", 0) or 0),
    )
