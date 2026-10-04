import json
from pathlib import Path
from typing import Any

import litellm
import mlflow
import pytest

from genai.llm import LLMConfig, complete_json, enable_tracing


def test_config_from_env() -> None:
    config = LLMConfig.from_env({"LITELLM_URL": "http://proxy:4000", "LITELLM_MASTER_KEY": "k"})
    assert (config.base_url, config.api_key, config.model) == ("http://proxy:4000", "k", "chat")
    assert LLMConfig.from_env({}).base_url == "http://127.0.0.1:4000"
    assert LLMConfig.from_env({}).trace is False
    assert LLMConfig.from_env({"MLFLOW_TRACKING_URI": "http://mlflow:5000"}).trace is True


def test_complete_json_calls_the_proxy_alias_with_a_json_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, Any] = {}
    real = litellm.completion

    def spy(**kwargs: Any) -> Any:
        seen.update(kwargs)
        return real(**kwargs)

    monkeypatch.setattr(litellm, "completion", spy)
    config = LLMConfig(base_url="http://proxy:4000", api_key="k")
    schema = {"type": "object", "properties": {"a": {"type": "integer"}}}
    out = complete_json(
        config,
        [{"role": "user", "content": "hi"}],
        schema,
        "thing",
        mock_response=json.dumps({"a": 1}),
    )
    assert json.loads(out.text) == {"a": 1}
    assert out.completion_tokens > 0
    assert seen["model"] == "litellm_proxy/chat"
    assert seen["api_base"] == "http://proxy:4000"
    assert seen["api_key"] == "k"
    assert seen["num_retries"] == config.num_retries
    assert seen["response_format"] == {
        "type": "json_schema",
        "json_schema": {"name": "thing", "schema": schema, "strict": True},
    }


def test_trace_is_tagged_with_prompt_hash_and_alias(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MLFLOW_ALLOW_FILE_STORE", "true")
    monkeypatch.setenv("MLFLOW_ENABLE_ASYNC_TRACE_LOGGING", "false")
    mlflow.set_tracking_uri(tmp_path.as_uri())
    enable_tracing()
    try:
        complete_json(
            LLMConfig(trace=True),
            [{"role": "user", "content": "hi"}],
            {"type": "object"},
            "product_enrichment",
            tags={"prompt_hash": "abc"},
            mock_response="{}",
        )
        (trace,) = mlflow.search_traces(return_type="list")
    finally:
        mlflow.litellm.autolog(disable=True)
        mlflow.set_tracking_uri(None)  # type: ignore[arg-type]
    assert trace.info.tags["prompt_hash"] == "abc"
    assert trace.info.tags["model_alias"] == "chat"
    assert [s.name for s in trace.data.spans] == ["product_enrichment", "litellm-completion"]
