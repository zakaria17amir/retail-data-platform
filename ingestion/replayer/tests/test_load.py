from pathlib import Path

import psycopg
import pytest
from conftest import REVIEW_MESSAGE
from replayer.load import row_counts, seed
from replayer.schema import TABLES

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


def test_copy_rejects_mismatched_header(dsn: str, tmp_csv_dir: Path) -> None:
    before = seed(dsn, tmp_csv_dir)
    path = tmp_csv_dir / "olist_customers_dataset.csv"
    lines = path.read_text(encoding="utf-8").splitlines()
    cols = lines[0].split(",")
    cols[0], cols[1] = cols[1], cols[0]
    path.write_text("\n".join([",".join(cols), *lines[1:]]) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="olist_customers_dataset.csv: header"):
        seed(dsn, tmp_csv_dir)
    assert row_counts(dsn) == before


def test_updated_at_trigger_fires(dsn: str, conn: psycopg.Connection, tmp_csv_dir: Path) -> None:
    seed(dsn, tmp_csv_dir)
    query = "select updated_at from olist.customers where customer_id = 'c1'"
    before = conn.execute(query).fetchone()
    conn.execute("update olist.customers set customer_city = 'osasco' where customer_id = 'c1'")
    after = conn.execute(query).fetchone()
    assert before is not None
    assert after is not None
    assert after[0] > before[0]
