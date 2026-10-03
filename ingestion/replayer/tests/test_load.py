from pathlib import Path

import psycopg
import pytest
from conftest import REVIEW_MESSAGE
from replayer.load import copy_csv, row_counts, seed
from replayer.schema import TABLES, apply_schema

EXPECTED = {
    "product_category_name_translation": 1,
    "customers": 2,
    "sellers": 1,
    "products": 2,
    "geolocation": 2,
    "orders": 2,
    "order_items": 3,
    "order_payments": 2,
    "order_reviews": 3,
}

pytestmark = pytest.mark.integration


def test_seed_loads_counts(dsn: str, tmp_csv_dir: Path) -> None:
    assert seed(dsn, tmp_csv_dir) == EXPECTED
    assert row_counts(dsn) == EXPECTED
    assert list(EXPECTED) == [t.name for t in TABLES]


def test_copy_handles_quoted_newlines(
    dsn: str, conn: psycopg.Connection, tmp_csv_dir: Path
) -> None:
    seed(dsn, tmp_csv_dir)
    row = conn.execute(
        "select review_comment_message from olist.order_reviews where review_id = 'r1'"
    ).fetchone()
    assert row is not None
    assert row[0] == REVIEW_MESSAGE


def test_duplicate_review_ids_load(dsn: str, conn: psycopg.Connection, tmp_csv_dir: Path) -> None:
    seed(dsn, tmp_csv_dir)
    row = conn.execute("select count(*) from olist.order_reviews where review_id = 'r2'").fetchone()
    assert row == (2,)
    assert row_counts(dsn)["order_reviews"] == 3


def test_seed_is_idempotent(dsn: str, tmp_csv_dir: Path) -> None:
    first = seed(dsn, tmp_csv_dir)
    assert seed(dsn, tmp_csv_dir) == first


def test_copy_strips_leading_bom(dsn: str, conn: psycopg.Connection, tmp_path: Path) -> None:
    apply_schema(conn)
    table = next(t for t in TABLES if t.name == "sellers")
    path = tmp_path / table.csv_file
    header = ",".join(table.columns).encode()
    path.write_bytes(b"\xef\xbb\xbf" + header + b"\ns1,13023,campinas,SP\n")
    conn.execute("truncate olist.sellers")
    assert copy_csv(conn, table, path) == 1
    row = conn.execute("select seller_id from olist.sellers").fetchone()
    assert row == ("s1",)


def test_updated_at_trigger_fires(dsn: str, conn: psycopg.Connection, tmp_csv_dir: Path) -> None:
    seed(dsn, tmp_csv_dir)
    query = "select updated_at from olist.customers where customer_id = 'c1'"
    before = conn.execute(query).fetchone()
    conn.execute("update olist.customers set customer_city = 'osasco' where customer_id = 'c1'")
    after = conn.execute(query).fetchone()
    assert before is not None
    assert after is not None
    assert after[0] > before[0]
