"""Chainlit UI: one app, two chat profiles; the Chainlit thread id is the checkpointer thread."""

import asyncio
import json
import logging
import os
from contextlib import ExitStack
from typing import Any

import chainlit as cl
from chainlit.input_widget import InputWidget, TextInput
from langchain_core.messages import AIMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command

log = logging.getLogger(__name__)

DEFAULT_CLARIFICATION = "Use sensible defaults."
ASK_TIMEOUT_S = 300
PROFILES = {
    "Analytics": "Questions about revenue, orders, AOV and delivery, answered from the gold "
    "layer through the semantic layer, with the metric/SQL used and a chart.",
    "Shopping": "Find products, check stock, get recommendations, track your orders and place "
    "orders (every order needs your approval).",
}
GRAPHS: dict[tuple[str, ...], Any] = {}
_STACK = ExitStack()
_SAVER: list[Any] = []


def _checkpointer() -> Any:
    if not _SAVER:
        if os.environ.get("POSTGRES_DSN"):
            from agents.core.checkpoint import postgres_checkpointer

            _SAVER.append(_STACK.enter_context(postgres_checkpointer()))
        else:
            _SAVER.append(InMemorySaver())
    return _SAVER[0]


def _chat_alias() -> str:
    return "chat-hosted" if os.environ.get("LLM_PROVIDER") == "hosted" else "chat"


def _analytics(saver: Any, customer_id: str, thread_id: str) -> Any:
    from agents.analytics_agent.graph import build_graph
    from agents.analytics_agent.tools import default_registry
    from agents.core import llm

    return build_graph(llm.chat_model(_chat_alias()), default_registry(), checkpointer=saver)


def _shopping(saver: Any, customer_id: str, thread_id: str) -> Any:
    from agents.core import llm
    from agents.shopping_agent import db
    from agents.shopping_agent.graph import build_graph
    from agents.shopping_agent.tools import Session, build_registry

    shop = db.PostgresShop(db.read_dsn(), os.environ["SHOP_DSN"])
    registry = build_registry(shop, Session(customer_id=customer_id, session_id=thread_id))
    return build_graph(llm.chat_model(_chat_alias()), registry, saver)


BUILDERS = {"Analytics": _analytics, "Shopping": _shopping}


def graph_for(profile: str, customer_id: str, thread_id: str) -> Any:
    """Analytics is one shared graph; shopping tools are bound to the customer and thread."""
    key = (profile, customer_id, thread_id) if profile == "Shopping" else (profile,)
    if key not in GRAPHS:
        GRAPHS[key] = BUILDERS[profile](_checkpointer(), customer_id, thread_id)
    return GRAPHS[key]


def chat_settings() -> cl.ChatSettings:
    customer: InputWidget = TextInput(
        id="customer_id",
        label="Customer id",
        initial=os.environ.get("DEMO_CUSTOMER_ID", ""),
        description="Olist customer_id the shopping assistant acts for.",
    )
    return cl.ChatSettings([customer])


def customer_id(settings: dict[str, Any] | None) -> str:
    chosen = str((settings or {}).get("customer_id") or "").strip()
    return chosen or os.environ.get("DEMO_CUSTOMER_ID", "")


def thread_config(thread_id: str) -> dict[str, Any]:
    return {"configurable": {"thread_id": thread_id}}


def payload(profile: str, text: str, thread_id: str) -> dict[str, Any]:
    if profile == "Shopping":
        return {"question": text, "session_id": thread_id}
    return {"question": text}


def summarize(node: str, update: dict[str, Any]) -> str:
    """Markdown for one graph step: tool calls with args, the SQL run, errors, issues."""
    lines: list[str] = []
    for msg in update.get("messages") or []:
        if isinstance(msg, AIMessage) and msg.tool_calls:
            lines += [f"Calling `{c['name']}`" for c in msg.tool_calls]
    for run in update.get("runs") or []:
        lines.append(f"**{run['name']}** `{json.dumps(run['args'], default=str)}`")
        if run.get("error"):
            lines.append(f"ERROR: {run['error']}")
        elif isinstance(run.get("output"), dict) and run["output"].get("sql"):
            lines.append(f"```sql\n{run['output']['sql']}\n```")
    if update.get("pending_question"):
        lines.append(f"Clarifying: {update['pending_question']}")
    if update.get("issue"):
        lines.append(f"Issue: {update['issue']}")
    if update.get("blocked"):
        lines.append("Blocked by the input guardrail.")
    return "\n\n".join(lines)


async def _stream(graph: Any, current: Any, config: dict[str, Any]) -> Any:
    """Stream graph updates as Chainlit steps; returns the pending interrupt value, if any."""
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[Any] = asyncio.Queue()
    done = object()

    def produce() -> None:
        try:
            for chunk in graph.stream(current, config, stream_mode="updates"):
                loop.call_soon_threadsafe(queue.put_nowait, chunk)
        except Exception as exc:  # noqa: BLE001 - re-raised on the event loop below
            loop.call_soon_threadsafe(queue.put_nowait, exc)
        finally:
            loop.call_soon_threadsafe(queue.put_nowait, done)

    worker = loop.run_in_executor(None, produce)
    pending, error = None, None
    while (chunk := await queue.get()) is not done:
        if isinstance(chunk, Exception):
            error = chunk
            continue
        for node, update in chunk.items():
            if node == "__interrupt__":
                pending = update[0].value
                continue
            async with cl.Step(name=node, type="tool") as step:
                step.output = summarize(node, update or {})
    await worker
    if error is not None:
        raise error
    return pending


async def _resolve(value: Any) -> Any:
    """Clarify interrupts ask the user; any other interrupt is the place_order approval."""
    if isinstance(value, dict) and "clarify" in value:
        reply = await cl.AskUserMessage(content=value["clarify"], timeout=ASK_TIMEOUT_S).send()
        return reply["output"] if reply else DEFAULT_CLARIFICATION
    details = json.dumps(value, indent=2, default=str)
    res = await cl.AskActionMessage(
        content=f"Approve this order?\n```json\n{details}\n```",
        actions=[
            cl.Action(name="approve", payload={"approved": True}, label="Approve"),
            cl.Action(name="reject", payload={"approved": False}, label="Reject"),
        ],
        timeout=ASK_TIMEOUT_S,
    ).send()
    return {"approved": bool(res and res["payload"].get("approved"))}


@cl.set_chat_profiles
async def chat_profiles(user: cl.User | None) -> list[cl.ChatProfile]:
    return [cl.ChatProfile(name=n, markdown_description=d) for n, d in PROFILES.items()]


@cl.on_chat_start
async def on_chat_start() -> None:
    from agents.core import tracing

    if cl.user_session.get("chat_profile") == "Shopping":  # type: ignore[no-untyped-call]
        await chat_settings().send()  # type: ignore[no-untyped-call]
    if tracing._ENABLED:
        return
    try:
        await asyncio.to_thread(tracing.enable_tracing)
    except Exception as exc:  # noqa: BLE001 - the UI works without MLflow
        log.warning("tracing_disabled", extra={"reason": str(exc)})


@cl.on_message
async def on_message(message: cl.Message) -> None:
    profile = cl.user_session.get("chat_profile") or "Analytics"  # type: ignore[no-untyped-call]
    thread_id = cl.context.session.thread_id
    config = thread_config(thread_id)
    settings = cl.user_session.get("chat_settings")  # type: ignore[no-untyped-call]
    graph = graph_for(profile, customer_id(settings), thread_id)
    current: Any = payload(profile, message.content, thread_id)
    while (pending := await _stream(graph, current, config)) is not None:
        current = Command(resume=await _resolve(pending))
    state = graph.get_state(config).values
    chart = state.get("chart")
    elements = [cl.Image(path=chart, name="chart", display="inline")] if chart else []
    reply = cl.Message(content=state.get("answer", ""), elements=elements)
    await reply.send()  # type: ignore[no-untyped-call]
