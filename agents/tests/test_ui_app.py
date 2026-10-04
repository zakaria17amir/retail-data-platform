import asyncio
import importlib
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest


@pytest.fixture
def app(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    """Import the app with Chainlit rooted in tmp (chainlit writes .chainlit/ on import)."""
    monkeypatch.setenv("CHAINLIT_APP_ROOT", str(tmp_path))
    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    for name in [m for m in sys.modules if m == "chainlit" or m.startswith("chainlit.")]:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.delitem(sys.modules, "agents.ui.app", raising=False)
    return importlib.import_module("agents.ui.app")


def test_app_declares_analytics_and_shopping_profiles(app: ModuleType) -> None:
    profiles = asyncio.run(app.chat_profiles(None))
    assert [p.name for p in profiles] == ["Analytics", "Shopping"]


def test_step_summaries_show_tool_calls_sql_and_metric(app: ModuleType) -> None:
    update: dict[str, Any] = {
        "runs": [
            {
                "name": "query_metric",
                "args": {"metrics": ["revenue"], "group_by": ["metric_time__year"]},
                "output": {"sql": "SELECT 1", "columns": ["revenue"], "rows": [[1.0]]},
                "error": None,
            },
            {"name": "run_sql", "args": {"sql": "drop x"}, "output": None, "error": "denied"},
        ]
    }
    text = app.summarize("execute", update)
    assert "query_metric" in text and "revenue" in text and "SELECT 1" in text
    assert "ERROR: denied" in text
    assert app.summarize("clarify", {"pending_question": ""}) == ""


def test_payload_carries_the_thread_as_shopping_session(app: ModuleType) -> None:
    assert app.payload("Analytics", "hi", "t1") == {"question": "hi"}
    shop = app.payload("Shopping", "hi", "t1")
    assert shop["question"] == "hi" and shop["session_id"] == "t1"


def test_thread_config_uses_the_chainlit_thread_id(app: ModuleType) -> None:
    assert app.thread_config("t1") == {"configurable": {"thread_id": "t1"}}


def test_profiles_build_graphs_with_one_shared_checkpointer(
    app: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    built: list[tuple[str, Any]] = []
    monkeypatch.setattr(
        app, "BUILDERS", {"Analytics": lambda saver, *_: built.append(("a", saver)) or "graph-a"}
    )
    app.GRAPHS.clear()
    assert app.graph_for("Analytics", "c1", "t1") == "graph-a"
    assert app.graph_for("Analytics", "c2", "t2") == "graph-a"
    assert len(built) == 1


def test_customer_id_setting_defaults_to_the_demo_customer(
    app: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DEMO_CUSTOMER_ID", "demo-1")
    (widget,) = app.chat_settings().inputs
    assert (widget.id, widget.label, widget.initial) == ("customer_id", "Customer id", "demo-1")
    assert app.customer_id(None) == "demo-1"
    assert app.customer_id({"customer_id": " "}) == "demo-1"
    assert app.customer_id({"customer_id": " cust-9 "}) == "cust-9"


def test_shopping_graph_is_scoped_to_the_customer_and_thread(
    app: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    from conftest import call, fake_llm
    from langgraph.checkpoint.memory import InMemorySaver
    from shop_fakes import FakeShop

    from agents.core import llm
    from agents.shopping_agent import db

    shops: list[tuple[str, str]] = []

    def fake_shop(dsn: str, writer_dsn: str) -> FakeShop:
        shops.append((dsn, writer_dsn))
        return FakeShop()

    def reply(alias: str = "chat") -> Any:
        return fake_llm(call("get_order_status", {"order_id": "o-mine"}), "It was delivered.")

    monkeypatch.setenv("POSTGRES_DSN", "postgresql://reader@pg/retail")
    monkeypatch.setenv("SHOP_DSN", "postgresql://shop_writer@pg/retail")
    monkeypatch.setattr(app, "_SAVER", [InMemorySaver()])
    monkeypatch.setattr(llm, "chat_model", reply)
    monkeypatch.setattr(db, "PostgresShop", fake_shop)
    app.GRAPHS.clear()

    mine = app.graph_for("Shopping", "cust-1", "t1")
    assert app.graph_for("Shopping", "cust-1", "t1") is mine
    other = app.graph_for("Shopping", "cust-2", "t1")
    assert other is not mine
    assert shops[0] == ("postgresql://reader@pg/retail", "postgresql://shop_writer@pg/retail")
    config = app.thread_config("t1")
    out = mine.invoke(app.payload("Shopping", "Where is order o-mine?", "t1"), config)
    assert out["runs"][0]["error"] is None and out["answer"] == "It was delivered."
    out = other.invoke({"question": "Where is order o-mine?"}, app.thread_config("t2"))
    assert "not found for the current customer" in out["runs"][0]["error"]
