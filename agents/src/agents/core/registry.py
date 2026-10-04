import json
import logging
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from langchain_core.messages import ToolMessage
from pydantic import BaseModel

from agents.core.guardrails import wrap_untrusted

log = logging.getLogger(__name__)

MAX_TOOL_ERRORS = 3


class ToolError(Exception):
    """A tool failed in a way the model can fix by changing its arguments."""


class ToolNotAllowed(ToolError):
    pass


@dataclass(frozen=True)
class Tool:
    name: str
    description: str
    input_model: type[BaseModel]
    output_model: type[BaseModel]
    fn: Callable[[Any], BaseModel]
    agents: frozenset[str]


@dataclass(frozen=True)
class ToolRun:
    name: str
    args: dict[str, Any]
    output: dict[str, Any] | None = None
    error: str | None = None


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: dict[str, Tool] = {}

    def register(self, tool: Tool) -> None:
        if tool.name in self._tools:
            raise ValueError(f"tool {tool.name!r} is already registered")
        self._tools[tool.name] = tool

    def tools(self) -> list[Tool]:
        return list(self._tools.values())

    def tools_for(self, agent: str) -> list[Tool]:
        return [t for t in self._tools.values() if agent in t.agents]

    def specs(self, agent: str) -> list[dict[str, Any]]:
        return [
            {
                "type": "function",
                "function": {
                    "name": t.name,
                    "description": t.description,
                    "parameters": t.input_model.model_json_schema(),
                },
            }
            for t in self.tools_for(agent)
        ]

    def call(self, agent: str, name: str, args: Mapping[str, Any]) -> BaseModel:
        tool = self._tools.get(name)
        if tool is None or agent not in tool.agents:
            raise ToolNotAllowed(f"tool {name!r} is not allowed for agent {agent!r}")
        return tool.output_model.model_validate(tool.fn(tool.input_model.model_validate(args)))


def run_tool_calls(
    registry: ToolRegistry, agent: str, calls: Sequence[Mapping[str, Any]]
) -> tuple[list[ToolMessage], list[ToolRun]]:
    """Run LLM tool calls; any failure becomes an ERROR tool message the model can act on."""
    messages: list[ToolMessage] = []
    runs: list[ToolRun] = []
    for c in calls:
        name, args = str(c["name"]), dict(c.get("args") or {})
        start = time.perf_counter()
        try:
            out = registry.call(agent, name, args).model_dump(mode="json")
            run = ToolRun(name, args, output=out)
            content = wrap_untrusted(json.dumps(out, default=str))
        except Exception as exc:  # noqa: BLE001 - every tool failure is fed back to the model
            run = ToolRun(name, args, error=f"{type(exc).__name__}: {exc}")
            content = f"ERROR: {run.error}\nFix the arguments and call the tool again."
        log.info(
            "tool_call",
            extra={
                "agent": agent,
                "tool": name,
                "ok": run.error is None,
                "ms": round((time.perf_counter() - start) * 1000),
            },
        )
        runs.append(run)
        messages.append(ToolMessage(content=content, tool_call_id=str(c.get("id") or name)))
    return messages, runs
