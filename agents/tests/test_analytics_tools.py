from pathlib import Path

import pytest

from agents.analytics_agent.semantic import MetricFlowLayer, QueryMetricIn
from agents.analytics_agent.sql import GoldDuckDB, SqlRejected
from agents.analytics_agent.tools import (
    ModelDocsIn,
    PlotIn,
    build_registry,
    find_issue,
    get_model_docs,
    plot,
)
from agents.core.registry import ToolError


def test_list_metrics_from_semantic_manifest(dbt_target: Path, gold_dir: Path) -> None:
    layer = MetricFlowLayer(dbt_target / "semantic_manifest.json", GoldDuckDB(gold_dir))
    metrics = {m.name: m for m in layer.list_metrics().metrics}
    assert {"revenue", "orders", "aov", "late_delivery_rate"} <= set(metrics)
    assert "metric_time__day" in metrics["revenue"].dimensions
    assert "order__order_status" in metrics["orders"].dimensions


def test_query_metric_compiles_with_metricflow_and_runs_on_gold(
    dbt_target: Path, gold_dir: Path
) -> None:
    layer = MetricFlowLayer(dbt_target / "semantic_manifest.json", GoldDuckDB(gold_dir))
    out = layer.query(
        QueryMetricIn(
            metrics=["revenue", "orders"],
            group_by=["metric_time__year"],
            order_by=["metric_time__year"],
        )
    )
    assert out.columns == ["metric_time__year", "revenue", "orders"]
    assert [row[1:] for row in out.rows] == [[180.0, 2], [20.0, 1]]
    assert str(out.rows[0][0]).startswith("2017-01-01")
    assert "fct_order_items" in out.sql and '"main_marts"' not in out.sql
    where = layer.query(
        QueryMetricIn(
            metrics=["orders"],
            where=["{{ Dimension('order__order_status') }} = 'delivered'"],
        )
    )
    assert where.rows == [[2]]


def test_query_metric_bad_names_and_injected_where(dbt_target: Path, gold_dir: Path) -> None:
    layer = MetricFlowLayer(dbt_target / "semantic_manifest.json", GoldDuckDB(gold_dir))
    with pytest.raises(ToolError, match="revenu"):
        layer.query(QueryMetricIn(metrics=["revenu"]))
    with pytest.raises(SqlRejected):
        layer.query(
            QueryMetricIn(metrics=["orders"], where=["1=1) union select getenv('HOME') --"])
        )


def test_query_metric_limit_is_bounded() -> None:
    with pytest.raises(ValueError):
        QueryMetricIn(metrics=["orders"], limit=5000)


def test_get_model_docs(dbt_target: Path) -> None:
    docs = get_model_docs(dbt_target / "manifest.json", ModelDocsIn(name="fct_orders"))
    assert docs.description == "One row per order."
    assert docs.columns["order_status"] == "Status."
    with pytest.raises(ToolError, match="fct_orders"):
        get_model_docs(dbt_target / "manifest.json", ModelDocsIn(name="fct_order"))


def test_plot_writes_png(chart_dir: Path) -> None:
    out = plot(chart_dir, PlotIn(x=["2017", "2018"], y=[1.0, 2.0], title="Revenue", kind="line"))
    path = Path(out.path)
    assert path.exists() and path.read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"


def test_registry_exposes_the_analytics_allowlist(
    dbt_target: Path, gold_dir: Path, chart_dir: Path
) -> None:
    db = GoldDuckDB(gold_dir)
    reg = build_registry(
        MetricFlowLayer(dbt_target / "semantic_manifest.json", db),
        db,
        dbt_target / "manifest.json",
        chart_dir,
    )
    names = [t.name for t in reg.tools_for("analytics")]
    assert names == ["list_metrics", "query_metric", "get_model_docs", "run_sql", "plot"]
    assert reg.tools_for("shopping") == []


@pytest.mark.parametrize(
    ("columns", "rows", "issue"),
    [
        (["n"], [], "empty"),
        (["late_delivery_rate"], [[1.7]], "implausible"),
        (["revenue"], [[-5.0]], "implausible"),
        (["revenue"], [[5e13]], "implausible"),
        (["metric_time__year", "revenue"], [["2017", 10.0]], None),
        (["conversion_rate"], [[0.02]], None),
        (["revenue_growth"], [[-0.3]], None),
    ],
)
def test_find_issue(columns: list[str], rows: list[list[object]], issue: str | None) -> None:
    found = find_issue(columns, rows)
    assert (found is None) if issue is None else (found is not None and issue in found)
