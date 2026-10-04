from pathlib import Path

import pytest

from agents.analytics_agent.sql import GoldDuckDB, SqlRejected, guard_sql

TABLES = {"fct_orders", "fct_order_items"}


@pytest.mark.parametrize(
    "sql",
    [
        "insert into fct_orders values (1)",
        "update fct_orders set order_status = 'x'",
        "delete from fct_orders",
        "drop table fct_orders",
        "create table t as select 1",
        "alter table fct_orders add column x int",
        "select 1; select 2",
        "select * from fct_orders; drop table fct_orders",
        "copy fct_orders to 'out.csv'",
        "attach 'other.db'",
        "install httpfs",
        "load httpfs",
        "pragma version",
        "set enable_external_access = true",
        "select * from read_csv('C:/secrets.csv')",
        "select * from read_parquet('/etc/passwd')",
        "select * from 'data.csv'",
        "select * from glob('*')",
        "select * from query('drop table fct_orders')",
        "select getenv('HOME')",
        "select * from secret_table",
        "select * from other.main.fct_orders",
        "select * from fct_orders limit 5000",
        "select * from fct_orders limit (select 1)",
        "with x as (delete from fct_orders returning *) select * from x",
    ],
)
def test_guard_rejects(sql: str) -> None:
    with pytest.raises(SqlRejected):
        guard_sql(sql, TABLES)


def test_guard_adds_limit_and_allows_ctes_and_unions() -> None:
    assert guard_sql("select order_id from fct_orders", TABLES).endswith("LIMIT 1000")
    assert "LIMIT 10" in guard_sql("select * from fct_orders limit 10", TABLES)
    cte = (
        "with t as (select * from fct_orders) "
        "select count(*) from t join fct_order_items using (order_id)"
    )
    assert guard_sql(cte, TABLES)
    assert guard_sql("select 1 as a union all select 2", TABLES).endswith("LIMIT 1000")


def test_gold_duckdb_runs_read_only(gold_dir: Path) -> None:
    db = GoldDuckDB(gold_dir)
    assert {"fct_orders", "fct_order_items", "big"} <= db.tables
    out = db.run_sql("select order_status, count(*) as n from fct_orders group by 1 order by 1")
    assert out.columns == ["order_status", "n"]
    assert out.rows == [["canceled", 1], ["delivered", 2], ["shipped", 1]]
    assert out.sql.endswith("LIMIT 1000")
    with pytest.raises(SqlRejected):
        db.run_sql("drop view fct_orders")


def test_timestamptz_cells_are_iso_strings(gold_dir: Path) -> None:
    out = GoldDuckDB(gold_dir).run_sql(
        "select timestamptz '2017-01-05 10:00:00+00' as ts, 1.5::decimal(10, 2) as d"
    )
    assert out.rows == [["2017-01-05T10:00:00+00:00", 1.5]]


def test_gold_duckdb_blocks_file_access_even_past_the_parser(gold_dir: Path) -> None:
    db = GoldDuckDB(gold_dir)
    with pytest.raises(Exception, match="disabled by configuration|Permission"):
        db._con.execute("select * from read_csv('C:/Windows/win.ini')")
    with pytest.raises(Exception, match="read-only|locked"):
        db._con.execute("create table x as select 1")
    with pytest.raises(Exception, match="locked"):
        db._con.execute("set enable_external_access = true")


def test_explain_row_estimate_guard(gold_dir: Path) -> None:
    db = GoldDuckDB(gold_dir, max_estimated_rows=1_000_000)
    with pytest.raises(SqlRejected, match="estimated"):
        db.run_sql("select count(*) from big a, big b")


def test_timeout(gold_dir: Path) -> None:
    db = GoldDuckDB(gold_dir, timeout_s=0.001, max_estimated_rows=10**12)
    with pytest.raises(SqlRejected, match="timeout"):
        db.run_sql("select sum(a.x * b.x) from big a, big b")
