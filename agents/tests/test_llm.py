import pytest
from conftest import fake_llm
from langchain_core.messages import HumanMessage
from pydantic import BaseModel

from agents.core.llm import StructuredOutputError, chat_model, invoke_structured


class Answer(BaseModel):
    value: int


def test_structured_output_retries_with_the_validation_error() -> None:
    llm = fake_llm("not json", '```json\n{"value": 3}\n```')
    assert invoke_structured(llm, [HumanMessage(content="q")], Answer) == Answer(value=3)
    assert "Invalid JSON" in str(llm.seen[1][-1].content)


def test_structured_output_rejects_after_three_attempts() -> None:
    llm = fake_llm("x", '{"value": "a"}', "{}")
    with pytest.raises(StructuredOutputError, match="3 attempts"):
        invoke_structured(llm, [HumanMessage(content="q")], Answer)


def test_chat_model_targets_the_litellm_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LITELLM_MASTER_KEY", raising=False)
    with pytest.raises(RuntimeError):
        chat_model()
    monkeypatch.setenv("LITELLM_MASTER_KEY", "sk-test")
    monkeypatch.setenv("LITELLM_URL", "http://litellm:4000")
    model = chat_model("judge")
    assert model.model_name == "judge"  # type: ignore[attr-defined]
    assert model.openai_api_base == "http://litellm:4000"  # type: ignore[attr-defined]
