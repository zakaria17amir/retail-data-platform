import pytest
from langchain_core.messages import ToolMessage
from pydantic import BaseModel

from agents.core.registry import Tool, ToolNotAllowed, ToolRegistry, run_tool_calls


class EchoIn(BaseModel):
    text: str


class EchoOut(BaseModel):
    text: str


def echo(args: EchoIn) -> EchoOut:
    if args.text == "boom":
        raise ValueError("boom happened")
    return EchoOut(text=args.text.upper())


def registry() -> ToolRegistry:
    reg = ToolRegistry()
    reg.register(Tool("echo", "Echo text.", EchoIn, EchoOut, echo, agents=frozenset({"analytics"})))
    reg.register(
        Tool("secret", "Shop only.", EchoIn, EchoOut, echo, agents=frozenset({"shopping"}))
    )
    return reg


def test_allowlist_per_agent() -> None:
    reg = registry()
    assert [t.name for t in reg.tools_for("analytics")] == ["echo"]
    assert [s["function"]["name"] for s in reg.specs("shopping")] == ["secret"]
    assert reg.specs("analytics")[0]["function"]["parameters"]["required"] == ["text"]
    assert reg.call("analytics", "echo", {"text": "hi"}) == EchoOut(text="HI")
    with pytest.raises(ToolNotAllowed):
        reg.call("analytics", "secret", {"text": "hi"})
    with pytest.raises(ToolNotAllowed):
        reg.call("analytics", "unknown", {"text": "hi"})


def test_duplicate_registration_fails() -> None:
    reg = registry()
    with pytest.raises(ValueError):
        reg.register(Tool("echo", "again", EchoIn, EchoOut, echo, agents=frozenset({"x"})))


def test_run_tool_calls_feeds_errors_back() -> None:
    reg = registry()
    calls = [
        {"name": "echo", "args": {"text": "hi"}, "id": "1"},
        {"name": "echo", "args": {}, "id": "2"},
        {"name": "echo", "args": {"text": "boom"}, "id": "3"},
        {"name": "secret", "args": {"text": "x"}, "id": "4"},
    ]
    messages, runs = run_tool_calls(reg, "analytics", calls)
    assert all(isinstance(m, ToolMessage) for m in messages)
    assert [m.tool_call_id for m in messages] == ["1", "2", "3", "4"]
    assert [r.error is None for r in runs] == [True, False, False, False]
    assert runs[0].output == {"text": "HI"}
    assert "text" in (runs[1].error or "")
    assert "boom happened" in (runs[2].error or "")
    assert "not allowed" in (runs[3].error or "")
    assert messages[1].content.startswith("ERROR")
    assert "<data>" in str(messages[0].content)
