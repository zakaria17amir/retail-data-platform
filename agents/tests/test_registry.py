import pytest
from langchain_core.messages import ToolMessage
from pydantic import BaseModel

from agents.core.registry import (
    MAX_LLM_ROWS,
    Tool,
    ToolNotAllowed,
    ToolRegistry,
    run_tool_calls,
)


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


class TableOut(BaseModel):
    columns: list[str]
    rows: list[list[int]]


def test_tool_rows_fed_to_the_llm_are_capped_but_the_run_keeps_them_all() -> None:
    reg = ToolRegistry()
    big = TableOut(columns=["x"], rows=[[i] for i in range(MAX_LLM_ROWS + 25)])
    reg.register(Tool("table", "Rows.", EchoIn, TableOut, lambda _: big, frozenset({"a"})))
    messages, runs = run_tool_calls(reg, "a", [{"name": "table", "args": {"text": ""}, "id": "1"}])
    assert runs[0].output == big.model_dump()
    content = str(messages[0].content)
    assert f"[{MAX_LLM_ROWS - 1}]" in content and f"[{MAX_LLM_ROWS}]" not in content
    assert "25 more rows omitted" in content


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
