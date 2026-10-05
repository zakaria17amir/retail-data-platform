import os
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from agents.shopping_agent.tools import (
    SHIPPING_LIMIT,
    OrderRow,
    ProductRow,
    Quote,
)

REPO_ROOT = Path(__file__).resolve().parents[4]
SHOP_SQL = REPO_ROOT / "sql" / "shop.sql"
OLIST_SCHEMA_SQL = (
    REPO_ROOT / "ingestion" / "replayer" / "src" / "replayer" / "sql" / "olist_schema.sql"
)

PRODUCTS = """
SELECT p.product_id, p.product_category_name AS category,
       t.product_category_name_english AS category_en,
       p.product_photos_qty AS photos, p.product_weight_g AS weight_g,
       o.price::float AS price, o.freight_value::float AS freight, o.seller_id
FROM olist.products p
LEFT JOIN olist.product_category_name_translation t USING (product_category_name)
LEFT JOIN LATERAL (
    SELECT i.price, i.freight_value, i.seller_id FROM olist.order_items i
    WHERE i.product_id = p.product_id
    ORDER BY i.shipping_limit_date DESC NULLS LAST, i.order_id LIMIT 1
) o ON true
WHERE p.product_id = ANY(%s)
"""
REVIEWS = """
SELECT txt FROM (
    SELECT DISTINCT concat_ws('. ', nullif(trim(r.review_comment_title), ''),
                              nullif(trim(r.review_comment_message), '')) AS txt
    FROM olist.order_items i JOIN olist.order_reviews r USING (order_id)
    WHERE i.product_id = %s
) s
WHERE txt <> '' ORDER BY length(txt) DESC, txt LIMIT 3
"""
ORDER = """
SELECT o.order_id, o.customer_id, c.customer_unique_id, o.order_status AS status,
       o.order_purchase_timestamp AS purchased_at, o.order_approved_at AS approved_at,
       o.order_delivered_carrier_date AS shipped_at,
       o.order_delivered_customer_date AS delivered_at,
       o.order_estimated_delivery_date AS estimated_delivery
FROM olist.orders o LEFT JOIN olist.customers c USING (customer_id)
WHERE o.order_id = %s
"""


def read_dsn() -> str:
    return os.environ.get("POSTGRES_DSN") or os.environ["RAG_DSN"]


def init_shop(dsn: str) -> int:
    """Apply sql/shop.sql (schema shop, seeded stock, role shop_writer); returns stock rows."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(SHOP_SQL.read_text(encoding="utf-8"))
        row = conn.execute("SELECT count(*) FROM shop.stock").fetchone()
    return int(row[0]) if row else 0


class PostgresShop:
    """Reads with the read DSN; order inserts only through the INSERT-only shop_writer role."""

    def __init__(self, dsn: str, writer_dsn: str) -> None:
        self.dsn = dsn
        self.writer_dsn = writer_dsn

    def _rows(self, query: str, params: Sequence[Any]) -> list[dict[str, Any]]:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            return conn.execute(query, params).fetchall()

    def products(self, ids: Sequence[str]) -> dict[str, ProductRow]:
        rows = self._rows(PRODUCTS, [list(ids)])
        return {r["product_id"]: ProductRow.model_validate(r) for r in rows}

    def reviews(self, product_id: str) -> list[str]:
        return [r["txt"] for r in self._rows(REVIEWS, [product_id])]

    def stock(self, ids: Sequence[str]) -> dict[str, int]:
        rows = self._rows(
            "SELECT product_id, on_hand FROM shop.stock WHERE product_id = ANY(%s)", [list(ids)]
        )
        return {r["product_id"]: r["on_hand"] for r in rows}

    def order(self, order_id: str) -> OrderRow | None:
        rows = self._rows(ORDER, [order_id])
        return OrderRow.model_validate(rows[0]) if rows else None

    def customer_unique_id(self, customer_id: str) -> str | None:
        rows = self._rows(
            "SELECT customer_unique_id FROM olist.customers WHERE customer_id = %s", [customer_id]
        )
        return (rows[0]["customer_unique_id"] or customer_id) if rows else None

    def dataset_now(self) -> datetime:
        rows = self._rows("SELECT max(order_purchase_timestamp) AS t FROM olist.orders", [])
        latest: datetime | None = rows[0]["t"]
        return latest or datetime.now(UTC).replace(tzinfo=None)

    def insert_order(self, order_id: str, quote: Quote) -> None:
        """One transaction shaped like a replayer insert: status 'created', no approval yet."""
        units = [ln for ln in quote.lines for _ in range(ln.quantity)]
        limit = quote.purchased_at + SHIPPING_LIMIT
        with psycopg.connect(self.writer_dsn) as conn:
            conn.execute(
                "INSERT INTO olist.orders (order_id, customer_id, order_status,"
                " order_purchase_timestamp, order_estimated_delivery_date)"
                " VALUES (%s, %s, 'created', %s, %s)",
                [order_id, quote.customer_id, quote.purchased_at, quote.estimated_delivery],
            )
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO olist.order_items (order_id, order_item_id, product_id,"
                    " seller_id, shipping_limit_date, price, freight_value)"
                    " VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    [
                        [order_id, n, ln.product_id, ln.seller_id, limit, ln.unit_price, ln.freight]
                        for n, ln in enumerate(units, start=1)
                    ],
                )
            conn.execute(
                "INSERT INTO olist.order_payments (order_id, payment_sequential, payment_type,"
                " payment_installments, payment_value) VALUES (%s, 1, %s, %s, %s)",
                [order_id, quote.payment_type, quote.installments, quote.total],
            )
