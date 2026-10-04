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


def _shopping(message: str, customer: str, thread: str) -> None:
    from agents.core.checkpoint import postgres_checkpointer
    from agents.core.llm import chat_model
    from agents.core.tracing import enable_tracing
    from agents.shopping_agent.db import PostgresShop, read_dsn
    from agents.shopping_agent.graph import ask, build_graph
    from agents.shopping_agent.tools import Session, build_registry

    enable_tracing()
    alias = "chat-hosted" if os.environ.get("LLM_PROVIDER") == "hosted" else "chat"
    shop = PostgresShop(read_dsn(), os.environ["SHOP_DSN"])
    registry = build_registry(shop, Session(customer_id=customer, session_id=thread))
    saver_cm: Any = (
        postgres_checkpointer() if os.environ.get("POSTGRES_DSN") else nullcontext(InMemorySaver())
    )
    with saver_cm as saver:
        graph = build_graph(chat_model(alias), registry, checkpointer=saver)
        config = {"configurable": {"thread_id": thread}}
        payload: Any = {"question": message}
        while True:
            out = ask(graph, payload, config, model_alias=alias)
            if "__interrupt__" not in out:
                break
            print(out["__interrupt__"][0].value["quote"])
            reply = input("Approve this order? [y/N] > ").strip().lower()
            payload = Command(resume={"approved": reply == "y"})
    print(out["answer"])


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="agents")
    sub = parser.add_subparsers(dest="command", required=True)
    analytics = sub.add_parser("analytics", help="ask the analytics agent one question")
    analytics.add_argument("question")
    analytics.add_argument("--thread", default=None, help="checkpointer thread id")
    shopping = sub.add_parser("shopping", help="send one message to the shopping assistant")
    shopping.add_argument("message")
    shopping.add_argument("--customer", required=True, help="olist customer_id of the shopper")
    shopping.add_argument("--thread", default=None, help="chat/checkpointer thread id")
    shop = sub.add_parser("shop", help="shopping side tables")
    shop.add_argument("action", choices=["init"], help="apply sql/shop.sql (stock, shop_writer)")
    args = parser.parse_args(argv)
    configure_logging()
    if args.command == "analytics":
        _analytics(args.question, args.thread or uuid.uuid4().hex)
    elif args.command == "shopping":
        _shopping(args.message, args.customer, args.thread or uuid.uuid4().hex)
    elif args.command == "shop":
        from agents.shopping_agent.db import init_shop, read_dsn

        print(f"shop.stock rows: {init_shop(read_dsn())}")
