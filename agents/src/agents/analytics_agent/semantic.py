import threading
from pathlib import Path

import sqlglot
from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowQueryRequest
from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.duckdb_renderer import DuckDbSqlPlanRenderer
from metricflow_semantics.model.dbt_manifest_parser import (
    parse_manifest_from_dbt_generated_manifest,
)
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from pydantic import BaseModel, Field
from sqlglot import exp

from agents.analytics_agent.sql import MAX_LIMIT, GoldDuckDB, SqlRejected, TableOut
from agents.core.registry import ToolError


class ListMetricsIn(BaseModel):
    pass


class MetricInfo(BaseModel):
    name: str
    label: str
    description: str
    dimensions: list[str]


class MetricsOut(BaseModel):
    metrics: list[MetricInfo]


class QueryMetricIn(BaseModel):
    metrics: list[str] = Field(min_length=1, description="Metric names from list_metrics.")
    group_by: list[str] = Field(
        default_factory=list,
        description="Dimensions, e.g. metric_time__month, metric_time__year, order__order_status;"
        " time dimensions take a __day|__week|__month|__quarter|__year grain.",
    )
    where: list[str] = Field(
        default_factory=list,
        description="MetricFlow filters, e.g. {{ Dimension('order__order_status') }} = 'delivered'"
        " or {{ TimeDimension('metric_time', 'day') }} >= '2018-01-01'.",
    )
    order_by: list[str] = Field(
        default_factory=list, description="Metric or dimension names; prefix '-' for descending."
    )
    limit: int = Field(default=100, ge=1, le=MAX_LIMIT)


class _CompileOnlyClient:
    """MetricFlow only needs the dialect to compile; execution goes through GoldDuckDB."""

    sql_engine_type = SqlEngine.DUCKDB
    sql_plan_renderer = DuckDbSqlPlanRenderer()


def _unqualify(sql: str) -> str:
    """MetricFlow targets the warehouse relations; gold exposes the same models unqualified."""
    try:
        tree = sqlglot.parse_one(sql, read="duckdb")
    except sqlglot.errors.SqlglotError as exc:
        raise SqlRejected(f"cannot parse MetricFlow SQL: {exc}") from exc
    for table in tree.find_all(exp.Table):
        table.set("db", None)
        table.set("catalog", None)
    return tree.sql(dialect="duckdb")


class MetricFlowLayer:
    def __init__(self, semantic_manifest: Path, db: GoldDuckDB) -> None:
        self._path, self._db = semantic_manifest, db
        self._engine: MetricFlowEngine | None = None
        self._lock = threading.Lock()

    def _mf(self) -> MetricFlowEngine:
        with self._lock:
            if self._engine is None:
                manifest = parse_manifest_from_dbt_generated_manifest(
                    manifest_json_string=self._path.read_text(encoding="utf-8")
                )
                self._engine = MetricFlowEngine(
                    semantic_manifest_lookup=SemanticManifestLookup(manifest),
                    sql_client=_CompileOnlyClient(),  # type: ignore[arg-type]
                )
            return self._engine

    def list_metrics(self, _: ListMetricsIn | None = None) -> MetricsOut:
        return MetricsOut(
            metrics=[
                MetricInfo(
                    name=m.name,
                    label=m.label or m.name,
                    description=" ".join((m.description or "").split()),
                    dimensions=sorted({d.dunder_name for d in m.dimensions}),
                )
                for m in self._mf().list_metrics()
            ]
        )

    def compile(self, q: QueryMetricIn) -> str:
        request = MetricFlowQueryRequest.create(
            metric_names=q.metrics,
            group_by_names=q.group_by or None,
            where_constraints=q.where or None,
            order_by_names=q.order_by or None,
            limit=q.limit,
        )
        try:
            explained = self._mf().explain(mf_request=request)
        except Exception as exc:  # noqa: BLE001 - MetricFlow errors list the valid names
            raise ToolError(f"MetricFlow rejected the query: {exc}") from exc
        return _unqualify(explained.sql_statement.without_descriptions.sql)

    def query(self, q: QueryMetricIn) -> TableOut:
        return self._db.run_sql(self.compile(q))
