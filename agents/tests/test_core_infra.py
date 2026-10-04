import io
import json
import logging
import os
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import psycopg
import pytest
from conftest import call, fake_llm
from langgraph.types import Command
from psycopg import sql
from psycopg.conninfo import make_conninfo

from agents.analytics_agent.graph import ask, build_graph
from agents.analytics_agent.semantic import MetricFlowLayer
from agents.analytics_agent.sql import GoldDuckDB
from agents.analytics_agent.tools import build_registry
from agents.core import tracing
from agents.core.checkpoint import postgres_checkpointer
from agents.core.logs import JsonFormatter
from agents.core.tracing import enable_tracing, traced


@pytest.fixture(scope="session")
def dsn() -> Iterator[str]:
    base = os.environ.get("POSTGRES_DSN")
    if not base:
        pytest.skip("POSTGRES_DSN is not set")
    try:
        admin = psycopg.connect(base, autocommit=True, connect_timeout=3)
    except psycopg.OperationalError:
        pytest.skip("Postgres is unreachable")
    name = f"agents_test_{uuid.uuid4().hex[:8]}"
    with admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        try:
            yield make_conninfo(base, dbname=name)
        finally:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))


def test_postgres_checkpointer_resumes_in_a_new_process(
    dsn: str, dbt_target: Path, gold_dir: Path, chart_dir: Path
) -> None:
    db = GoldDuckDB(gold_dir)
    registry = build_registry(
        MetricFlowLayer(dbt_target / "semantic_manifest.json", db),
        db,
        dbt_target / "manifest.json",
        chart_dir,
    )
    config: Any = {"configurable": {"thread_id": "resume-1"}}
    clarify = json.dumps({"needs_clarification": True, "question": "Which year?"})
    with postgres_checkpointer(dsn) as saver:
        first = build_graph(fake_llm(clarify), registry, checkpointer=saver).invoke(
            {"question": "Revenue?"}, config
        )
    assert first["__interrupt__"][0].value == {"clarify": "Which year?"}

    replies = (call("query_metric", {"metrics": ["revenue"]}), "Revenue was 200.")
    with postgres_checkpointer(dsn) as saver:
        out = build_graph(fake_llm(*replies), registry, checkpointer=saver).invoke(
            Command(resume="all time"), config
        )
    assert out["answer"].startswith("Revenue was 200.")

    with psycopg.connect(dsn) as conn:
        tables = {
            r[0]
            for r in conn.execute(
                "select table_name from information_schema.tables where table_schema = 'agents'"
            )
        }
    assert {"checkpoints", "checkpoint_writes"} <= tables


def test_json_logs() -> None:
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter())
    log = logging.getLogger("agents.test")
    log.addHandler(handler)
    log.setLevel(logging.INFO)
    log.info("tool_call", extra={"tool": "run_sql", "ok": True})
    line = json.loads(stream.getvalue())
    assert line["msg"] == "tool_call" and line["tool"] == "run_sql" and line["ok"] is True
    assert line["level"] == "INFO" and "ts" in line


def test_agent_run_is_traced_with_prompt_hash_and_alias(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    dbt_target: Path,
    gold_dir: Path,
    chart_dir: Path,
) -> None:
    import mlflow

    monkeypatch.setattr(tracing, "_ENABLED", False)
    assert traced("analytics_agent", "prompt text", "chat", lambda: 1) == 1
    db = GoldDuckDB(gold_dir)
    registry = build_registry(
        MetricFlowLayer(dbt_target / "semantic_manifest.json", db),
        db,
        dbt_target / "manifest.json",
        chart_dir,
    )
    llm = fake_llm(
        json.dumps({"needs_clarification": False}),
        call("query_metric", {"metrics": ["revenue"]}),
        "Revenue was 200.",
    )
    enable_tracing(f"sqlite:///{(tmp_path / 'mlflow.db').as_posix()}", experiment="genai-test")
    try:
        out = ask(build_graph(llm, registry), {"question": "Total revenue?"}, model_alias="chat")
        assert out["answer"].startswith("Revenue was 200.")
        mlflow.flush_trace_async_logging()
        trace = mlflow.get_trace(mlflow.get_last_active_trace_id() or "")
        assert trace is not None
        assert trace.info.tags["model_alias"] == "chat"
        assert len(trace.info.tags["prompt_hash"]) == 12
        assert len(trace.data.spans) > 3
    finally:
        mlflow.langchain.autolog(disable=True)
