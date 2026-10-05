from collections import Counter
from pathlib import Path

import pytest

from agents.analytics_agent.semantic import MetricFlowLayer
from agents.analytics_agent.sql import GoldDuckDB
from agents.analytics_agent.tools import build_registry
from agents.core.registry import ToolRegistry
from agents.evals.golden import (
    ANALYTICS_GOLDEN,
    SHOPPING_GOLDEN,
    SHOPPING_TOOLS,
    SPECS,
    Spec,
    build_cases,
    load_jsonl,
    write_jsonl,
)


@pytest.fixture
def registry(dbt_target: Path, gold_dir: Path, chart_dir: Path) -> ToolRegistry:
    db = GoldDuckDB(gold_dir)
    layer = MetricFlowLayer(dbt_target / "semantic_manifest.json", db)
    return build_registry(layer, db, dbt_target / "manifest.json", chart_dir)


def test_expected_answers_come_from_the_agent_tools(registry: ToolRegistry, tmp_path: Path) -> None:
    specs = [
        Spec("Revenue by year?", "time_grain", "query_metric", {
            "metrics": ["revenue"], "group_by": ["metric_time__year"],
            "order_by": ["metric_time__year"]}),
        Spec("Total revenue?", "total", "query_metric", {"metrics": ["revenue"]}),
        Spec("Orders by status?", "sql_marts", "run_sql", {
            "sql": "select order_status, count(*) as n from fct_orders group by 1 order by 1"}),
    ]  # fmt: skip
    cases = build_cases(registry, specs)
    yearly, total, by_status = cases
    assert yearly["expected"] == {
        "columns": ["metric_time__year", "revenue"],
        "rows": [["2017", 180.0], ["2018", 20.0]],
    }
    assert total["expected"] == {"value": 200.0}
    assert by_status["expected"]["rows"] == [["canceled", 1], ["delivered", 2], ["shipped", 1]]
    assert all(c["tolerance"] == 0.005 for c in cases)
    assert yearly["reference"]["tool"] == "query_metric"
    assert "fct_order_items" in yearly["reference"]["sql"]
    assert [c["id"] for c in cases] == ["an-01", "an-02", "an-03"]
    path = tmp_path / "golden.jsonl"
    write_jsonl(path, cases)
    assert load_jsonl(path) == cases


def test_analytics_specs_mix() -> None:
    assert len(SPECS) == 60
    assert len({s.question for s in SPECS}) == 60
    counts = Counter(s.category for s in SPECS)
    assert set(counts) == {"time_grain", "by_dimension", "total", "filter", "top_n", "ratio",
                           "sql_marts"}  # fmt: skip
    assert min(counts.values()) >= 4
    assert sum(s.tool == "run_sql" for s in SPECS) >= 10


def test_committed_analytics_golden_matches_specs() -> None:
    cases = load_jsonl(ANALYTICS_GOLDEN)
    assert [c["question"] for c in cases] == [s.question for s in SPECS]
    assert all("value" in c["expected"] or c["expected"]["rows"] for c in cases)


def test_committed_shopping_golden_shape() -> None:
    cases = load_jsonl(SHOPPING_GOLDEN)
    counts = Counter(c["category"] for c in cases)
    assert counts == {"tool_selection": 40, "adversarial": 20}
    assert len({c["id"] for c in cases}) == 60
    for c in cases:
        assert c["question"] and c["customer_id"]
        tools = c.get("expected_tools", []) + c.get("forbidden_tools", [])
        assert set(tools) <= SHOPPING_TOOLS
    selected = Counter(t for c in cases for t in c.get("expected_tools", []))
    assert set(selected) == SHOPPING_TOOLS
    kinds = Counter(c["kind"] for c in cases if c["category"] == "adversarial")
    assert set(kinds) == {"injection", "indirect_injection", "pii_exfiltration",
                          "unapproved_order", "other_customer_order"}  # fmt: skip
    assert all(c.get("poison") for c in cases if c.get("kind") == "indirect_injection")
