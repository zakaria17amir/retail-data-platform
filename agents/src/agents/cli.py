import argparse
import os
import uuid
from contextlib import nullcontext
from typing import Any

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command

from agents.core.logs import configure_logging


def _analytics(question: str, thread: str) -> None:
    from agents.analytics_agent.graph import ask, build_graph
    from agents.analytics_agent.tools import default_registry
    from agents.core.checkpoint import postgres_checkpointer
    from agents.core.llm import chat_model
    from agents.core.tracing import enable_tracing

    enable_tracing()
    saver_cm: Any = (
        postgres_checkpointer() if os.environ.get("POSTGRES_DSN") else nullcontext(InMemorySaver())
    )
    with saver_cm as saver:
        graph = build_graph(chat_model(), default_registry(), checkpointer=saver)
        config = {"configurable": {"thread_id": thread}}
        payload: Any = {"question": question}
        while True:
            out = ask(graph, payload, config)
            if "__interrupt__" not in out:
                break
            payload = Command(resume=input(f"{out['__interrupt__'][0].value['clarify']} > "))
    print(out["answer"])
    if out.get("chart"):
        print(f"Chart: {out['chart']}")


def _eval(suite: str, provider: str, limit: int | None) -> None:
    if suite == "build-analytics":
        from agents.evals.golden import build_analytics

        build_analytics()
        return
    from agents.evals.runner import run_suite

    run_suite(suite, provider, limit)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="agents")
    sub = parser.add_subparsers(dest="command", required=True)
    analytics = sub.add_parser("analytics", help="ask the analytics agent one question")
    analytics.add_argument("question")
    analytics.add_argument("--thread", default=None, help="checkpointer thread id")
    evals = sub.add_parser("eval", help="build or run an eval golden set")
    evals.add_argument("suite", choices=["build-analytics", "analytics", "shopping"])
    evals.add_argument(
        "--provider",
        choices=["local", "hosted"],
        default=os.environ.get("LLM_PROVIDER", "local"),
    )
    evals.add_argument("--limit", type=int, default=None)
    args = parser.parse_args(argv)
    configure_logging()
    if args.command == "analytics":
        _analytics(args.question, args.thread or uuid.uuid4().hex)
    elif args.command == "eval":
        _eval(args.suite, args.provider, args.limit)
