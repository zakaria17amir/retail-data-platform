import difflib
import json
import os
import re
import tempfile
import uuid
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

from matplotlib.figure import Figure
from pydantic import BaseModel, Field

from agents.analytics_agent.semantic import (
    ListMetricsIn,
    MetricFlowLayer,
    MetricsOut,
    QueryMetricIn,
)
from agents.analytics_agent.sql import GoldDuckDB, TableOut
from agents.core.registry import Tool, ToolError, ToolRegistry

AGENT = "analytics"
REPO_ROOT = Path(__file__).resolve().parents[4]
ALLOWED = frozenset({AGENT})
DATA_TOOLS = ("query_metric", "run_sql")


def gold_dir() -> Path:
    return Path(os.environ.get("GOLD_DIR") or REPO_ROOT / "data" / "gold")


def dbt_target() -> Path:
    return Path(os.environ.get("DBT_PROJECT_DIR") or REPO_ROOT / "analytics") / "target"


def chart_dir() -> Path:
    return Path(os.environ.get("CHART_DIR") or Path(tempfile.gettempdir()) / "agents_charts")


class RunSqlIn(BaseModel):
    sql: str = Field(description="One DuckDB SELECT over the gold tables; LIMIT <= 1000.")


class ModelDocsIn(BaseModel):
    name: str = Field(description="dbt model name, e.g. fct_orders.")


class ModelDocsOut(BaseModel):
    name: str
    description: str
    columns: dict[str, str]


class PlotIn(BaseModel):
    x: list[str] = Field(min_length=1, max_length=1000)
    y: list[float] = Field(min_length=1, max_length=1000)
    title: str = ""
    kind: Literal["line", "bar"] = "bar"


class PlotOut(BaseModel):
    path: str


@lru_cache(maxsize=4)
def _models(manifest: Path) -> dict[str, dict[str, Any]]:
    nodes = json.loads(manifest.read_text(encoding="utf-8"))["nodes"].values()
    return {n["name"]: n for n in nodes if n.get("resource_type") == "model"}


def get_model_docs(manifest: Path, args: ModelDocsIn) -> ModelDocsOut:
    models = _models(manifest)
    node = models.get(args.name)
    if node is None:
        close = difflib.get_close_matches(args.name, models, n=5, cutoff=0.5)
        raise ToolError(f"unknown model {args.name!r}; did you mean: {', '.join(close)}")
    return ModelDocsOut(
        name=args.name,
        description=node.get("description", ""),
        columns={c["name"]: c.get("description", "") for c in node.get("columns", {}).values()},
    )


def plot(directory: Path, args: PlotIn) -> PlotOut:
    if len(args.x) != len(args.y):
        raise ToolError("x and y must have the same length")
    directory.mkdir(parents=True, exist_ok=True)
    fig = Figure(figsize=(8, 4))
    ax = fig.subplots()
    if args.kind == "line":
        ax.plot(args.x, args.y, marker="o")
    else:
        ax.bar(args.x, args.y)
    ax.set_title(args.title)
    ax.tick_params(axis="x", labelrotation=45)
    fig.tight_layout()
    path = directory / f"{uuid.uuid4().hex}.png"
    fig.savefig(path, format="png", dpi=100)
    return PlotOut(path=str(path))


_RATE = re.compile(r"(rate|ctr|ratio|share)$")
_NON_NEGATIVE = re.compile(r"(revenue|orders|count|sessions|price|value|^n)$")
MAX_MAGNITUDE = 1e12


def find_issue(columns: list[str], rows: list[list[Any]]) -> str | None:
    """Why a result looks wrong (empty, rate outside [0, 1], negative count, absurd size)."""
    if not rows:
        return "empty result: check metric/dimension names, filters and date ranges"
    for row in rows:
        for col, value in zip(columns, row, strict=False):
            if not isinstance(value, int | float) or isinstance(value, bool):
                continue
            name = col.lower()
            if _RATE.search(name) and not 0 <= value <= 1:
                return f"implausible magnitude: {col}={value} is a rate outside [0, 1]"
            if _NON_NEGATIVE.search(name) and value < 0:
                return f"implausible magnitude: {col}={value} should not be negative"
            if abs(value) > MAX_MAGNITUDE:
                return f"implausible magnitude: {col}={value} is too large"
    return None


def build_registry(
    semantic: MetricFlowLayer, db: GoldDuckDB, manifest: Path, charts: Path
) -> ToolRegistry:
    reg = ToolRegistry()
    tools = [
        Tool(
            "list_metrics",
            "List semantic-layer metrics with their descriptions and dimensions.",
            ListMetricsIn,
            MetricsOut,
            semantic.list_metrics,
            ALLOWED,
        ),
        Tool(
            "query_metric",
            "Query metrics through the MetricFlow semantic layer. Prefer this over run_sql.",
            QueryMetricIn,
            TableOut,
            semantic.query,
            ALLOWED,
        ),
        Tool(
            "get_model_docs",
            "Describe a dbt gold model and its columns.",
            ModelDocsIn,
            ModelDocsOut,
            lambda a: get_model_docs(manifest, a),
            ALLOWED,
        ),
        Tool(
            "run_sql",
            f"Read-only DuckDB SELECT over gold tables: {', '.join(sorted(db.tables))}.",
            RunSqlIn,
            TableOut,
            lambda a: db.run_sql(a.sql),
            ALLOWED,
        ),
        Tool(
            "plot", "Plot x/y as a PNG chart.", PlotIn, PlotOut, lambda a: plot(charts, a), ALLOWED
        ),
    ]
    for tool in tools:
        reg.register(tool)
    return reg


def default_registry() -> ToolRegistry:
    db = GoldDuckDB(gold_dir())
    target = dbt_target()
    layer = MetricFlowLayer(target / "semantic_manifest.json", db)
    return build_registry(layer, db, target / "manifest.json", chart_dir())
