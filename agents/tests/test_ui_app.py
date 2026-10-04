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
        app, "BUILDERS", {"Analytics": lambda saver: built.append(("a", saver)) or "graph-a"}
    )
    app.GRAPHS.clear()
    assert app.graph_for("Analytics") == "graph-a"
    assert app.graph_for("Analytics") == "graph-a"
    assert len(built) == 1
