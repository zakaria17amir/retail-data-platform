import datetime as dt
import json
import math
import re
import tempfile
import threading
from collections.abc import Collection
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import sqlglot
from pydantic import BaseModel
from sqlglot import exp

from agents.core.registry import ToolError

MAX_LIMIT = 1000
TIMEOUT_S = 10.0
MAX_ESTIMATED_ROWS = 50_000_000

_DENIED_NODES = tuple(
    getattr(exp, name)
    for name in (
        "Insert", "Update", "Delete", "Merge", "Create", "Drop", "Alter", "Copy", "Command",
        "Into", "Pragma", "Set", "Use", "Attach", "Detach", "Install", "Export",
    )
    if hasattr(exp, name)
)  # fmt: skip
_DENIED_FUNCS = re.compile(
    r"^(read_\w+|\w+_scan|glob|query|query_table|getenv|sniff_csv|parquet_\w+|iceberg_\w+"
    r"|delta_\w+|postgres_\w+|mysql_\w+|sqlite_\w+|duckdb_secrets|load_aws_credentials)$"
)
_PRODUCT_NODES = {"CROSS_PRODUCT", "NESTED_LOOP_JOIN", "BLOCKWISE_NL_JOIN", "PIECEWISE_MERGE_JOIN"}

Cell = str | int | float | bool | None


class SqlRejected(ToolError):
    pass


class TableOut(BaseModel):
    sql: str
    columns: list[str]
    rows: list[list[Cell]]


def _func_name(node: exp.Func) -> str:
    return (node.name if isinstance(node, exp.Anonymous) else node.sql_name()).lower()


def guard_sql(sql: str, tables: Collection[str], max_limit: int = MAX_LIMIT) -> str:
    """One read-only SELECT over the gold tables, LIMIT <= max_limit; returns the SQL to run."""
    try:
        statements = [s for s in sqlglot.parse(sql, read="duckdb") if s is not None]
    except sqlglot.errors.SqlglotError as exc:
        raise SqlRejected(f"cannot parse SQL: {exc}") from exc
    if len(statements) != 1:
        raise SqlRejected("exactly one SQL statement is allowed")
    stmt = statements[0]
    if not isinstance(stmt, exp.Query):
        raise SqlRejected("only SELECT queries are allowed")
    for node in stmt.walk():
        if isinstance(node, _DENIED_NODES):
            raise SqlRejected(f"{node.key.upper()} is not allowed; read-only SELECT only")
        if isinstance(node, exp.Func) and _DENIED_FUNCS.match(_func_name(node)):
            raise SqlRejected(f"function {_func_name(node)}() is not allowed")
    ctes = {cte.alias_or_name.lower() for cte in stmt.find_all(exp.CTE)}
    allowed = {t.lower() for t in tables} | ctes
    for table in stmt.find_all(exp.Table):
        qualified = table.args.get("db") or table.args.get("catalog")
        if qualified or not isinstance(table.this, exp.Identifier):
            raise SqlRejected(f"use unqualified gold table names: {', '.join(sorted(tables))}")
        if table.name.lower() not in allowed:
            raise SqlRejected(
                f"unknown table {table.name!r}; gold tables: {', '.join(sorted(tables))}"
            )
    limit = stmt.args.get("limit")
    if limit is None:
        stmt = stmt.limit(max_limit)
    else:
        value = limit.expression
        if not (isinstance(value, exp.Literal) and value.is_int) or int(value.this) > max_limit:
            raise SqlRejected(f"LIMIT must be an integer <= {max_limit}")
    return stmt.sql(dialect="duckdb")


def estimate_rows(plan: Any) -> int:
    """Largest estimated cardinality in an EXPLAIN (FORMAT JSON) plan.

    DuckDB leaves cross products and nested-loop joins unestimated, so those multiply.
    An ungrouped aggregate (no GROUP BY) always emits one row, so MetricFlow's cross join of
    per-metric totals is cheap.
    """

    def walk(node: dict[str, Any]) -> tuple[int, int]:
        kids = [walk(c) for c in node.get("children", [])]
        info = node.get("extra_info")
        own = info.get("Estimated Cardinality") if isinstance(info, dict) else None
        if own is not None:
            rows = int(re.sub(r"\D", "", str(own)) or 0)
        elif node.get("name") == "UNGROUPED_AGGREGATE":
            rows = 1
        elif node.get("name") in _PRODUCT_NODES and kids:
            rows = math.prod(k[0] for k in kids)
        else:
            rows = max((k[0] for k in kids), default=0)
        return rows, max([rows, *(k[1] for k in kids)])

    nodes = plan if isinstance(plan, list) else [plan]
    return max((walk(n)[1] for n in nodes), default=0)


def _cell(value: object) -> Cell:
    if isinstance(value, dt.date | dt.datetime | dt.time):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if value is None or isinstance(value, str | int | float | bool):
        return value
    return str(value)


class GoldDuckDB:
    """Read-only DuckDB catalogue of views over gold/*.parquet; no other file access."""

    def __init__(
        self,
        gold_dir: Path,
        timeout_s: float = TIMEOUT_S,
        max_estimated_rows: int = MAX_ESTIMATED_ROWS,
    ) -> None:
        self.timeout_s, self.max_estimated_rows = timeout_s, max_estimated_rows
        files = sorted(Path(gold_dir).resolve().glob("*.parquet"))
        self.tables = frozenset(f.stem for f in files)
        self._tmp = tempfile.TemporaryDirectory(prefix="agents_gold_", ignore_cleanup_errors=True)
        catalog = Path(self._tmp.name) / "gold.duckdb"
        with duckdb.connect(str(catalog)) as writer:
            for f in files:
                path = f.as_posix().replace("'", "''")
                writer.execute(f"CREATE VIEW \"{f.stem}\" AS SELECT * FROM read_parquet('{path}')")
        self._con = duckdb.connect(str(catalog), read_only=True)
        allowed = Path(gold_dir).resolve().as_posix().rstrip("/") + "/"
        self._con.execute("SET GLOBAL TimeZone = 'UTC'")
        self._con.execute("SET allowed_directories = ?", [[allowed]])
        self._con.execute("SET enable_external_access = false")
        # scalar range()/repeat() pass the guard; bound them before the timeout can fire
        self._con.execute("SET memory_limit = '512MiB'")
        self._con.execute("SET threads = 2")
        self._con.execute("SET max_temp_directory_size = '0B'")
        self._con.execute("SET lock_configuration = true")

    def run_sql(self, sql: str) -> TableOut:
        safe = guard_sql(sql, self.tables)
        cur = self._con.cursor()
        try:
            explained = cur.execute(f"EXPLAIN (FORMAT JSON) {safe}").fetchall()
            estimate = max((estimate_rows(json.loads(r[1])) for r in explained), default=0)
            if estimate > self.max_estimated_rows:
                raise SqlRejected(
                    f"query too expensive: estimated {estimate:,} rows "
                    f"(max {self.max_estimated_rows:,}); aggregate or filter first"
                )
            timer = threading.Timer(self.timeout_s, cur.interrupt)
            timer.start()
            try:
                cur.execute(safe)
                columns = [d[0] for d in cur.description or []]
                rows = cur.fetchall()
            except duckdb.InterruptException as exc:
                raise SqlRejected(f"query exceeded the {self.timeout_s:g} s timeout") from exc
            finally:
                timer.cancel()
        except duckdb.Error as exc:
            raise ToolError(f"{type(exc).__name__}: {exc}") from exc
        finally:
            cur.close()
        return TableOut(sql=safe, columns=columns, rows=[[_cell(v) for v in r] for r in rows])
