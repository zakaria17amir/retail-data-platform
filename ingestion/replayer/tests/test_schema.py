import os

import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict
from replayer.cli import main
from replayer.schema import TABLES, apply_schema

OLIST_FILES = {
    "olist_customers_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "product_category_name_translation.csv",
}


def test_tables_registry_covers_all_nine_csvs() -> None:
    assert {t.csv_file for t in TABLES} == OLIST_FILES


@pytest.mark.integration
def test_apply_schema_is_idempotent(conn: psycopg.Connection) -> None:
    apply_schema(conn)
    apply_schema(conn)
    rows = conn.execute(
        "select table_name from information_schema.tables where table_schema = 'olist'"
    ).fetchall()
    assert {r[0] for r in rows} == {t.name for t in TABLES}


@pytest.mark.integration
def test_publication_exists(conn: psycopg.Connection) -> None:
    apply_schema(conn)
    row = conn.execute("select 1 from pg_publication where pubname = 'olist_cdc'").fetchone()
    assert row is not None


def test_connection_error_hides_password(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://u:secret@nohost:1/db")
    assert main(["status"]) == 1
    err = capsys.readouterr().err
    assert "secret" not in err
    assert "error: could not connect to nohost:1/db" in err


@pytest.mark.integration
def test_integration_uses_throwaway_database(dsn: str) -> None:
    name = str(conninfo_to_dict(dsn)["dbname"])
    assert name.startswith("retail_test_")
    assert name != conninfo_to_dict(os.environ["POSTGRES_DSN"])["dbname"]
