import csv
import os
import uuid
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest
from dotenv import find_dotenv, load_dotenv
from psycopg import sql
from psycopg.conninfo import make_conninfo

load_dotenv(find_dotenv(usecwd=True))

REVIEW_MESSAGE = 'Chegou rápido, "muito bom",\nrecomendo'

FIXTURE: dict[str, tuple[list[str], list[list[object]]]] = {
    "olist_customers_dataset.csv": (
        [
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ],
        [
            ["c1", "u1", 1310, "sao paulo", "SP"],
            ["c2", "u2", 20040, "rio de janeiro", "RJ"],
        ],
    ),
    "olist_orders_dataset.csv": (
        [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
        [
            [
                "o1",
                "c1",
                "delivered",
                "2017-01-01 10:00:00",
                "2017-01-01 10:15:00",
                "2017-01-02 09:00:00",
                "2017-01-05 14:00:00",
                "2017-01-10 00:00:00",
            ],
            ["o2", "c2", "created", "2017-02-01 11:00:00", "", "", "", "2017-02-15 00:00:00"],
        ],
    ),
    "olist_order_items_dataset.csv": (
        [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
        ],
        [
            ["o1", 1, "p1", "s1", "2017-01-03 10:00:00", "29.99", "8.72"],
            ["o1", 2, "p2", "s1", "2017-01-03 10:00:00", "10.00", "8.72"],
            ["o2", 1, "p1", "s1", "2017-02-04 11:00:00", "29.99", "5.00"],
        ],
    ),
    "olist_order_payments_dataset.csv": (
        [
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value",
        ],
        [
            ["o1", 1, "credit_card", 2, "57.43"],
            ["o2", 1, "boleto", 1, "34.99"],
        ],
    ),
    "olist_order_reviews_dataset.csv": (
        [
            "review_id",
            "order_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_date",
            "review_answer_timestamp",
        ],
        [
            ["r1", "o1", 5, "", REVIEW_MESSAGE, "2017-01-06 00:00:00", "2017-01-07 10:00:00"],
            ["r2", "o2", 1, "ruim", "", "2017-02-16 00:00:00", "2017-02-17 10:00:00"],
            ["r2", "o1", 2, "", "", "2017-02-18 00:00:00", "2017-02-19 10:00:00"],
        ],
    ),
    "olist_products_dataset.csv": (
        [
            "product_id",
            "product_category_name",
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ],
        [
            ["p1", "beleza_saude", 40, 287, 1, 225, 16, 10, 14],
            ["p2", "", "", "", "", "", "", "", ""],
        ],
    ),
    "olist_sellers_dataset.csv": (
        ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
        [["s1", 13023, "campinas", "SP"]],
    ),
    "olist_geolocation_dataset.csv": (
        [
            "geolocation_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng",
            "geolocation_city",
            "geolocation_state",
        ],
        [
            [1037, "-23.545621", "-46.639292", "sao paulo", "SP"],
            [1046, "-23.546081", "-46.644820", "sao paulo", "SP"],
        ],
    ),
    "product_category_name_translation.csv": (
        ["product_category_name", "product_category_name_english"],
        [["beleza_saude", "health_beauty"]],
    ),
}


@pytest.fixture(scope="session")
def dsn() -> Iterator[str]:
    base = os.environ.get("POSTGRES_DSN")
    if not base:
        pytest.skip("POSTGRES_DSN is not set")
    try:
        admin = psycopg.connect(base, autocommit=True, connect_timeout=3)
    except psycopg.OperationalError:
        pytest.skip("Postgres is unreachable")
    name = f"retail_test_{uuid.uuid4().hex[:8]}"
    with admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        try:
            yield make_conninfo(base, dbname=name)
        finally:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))


@pytest.fixture
def conn(dsn: str) -> Iterator[psycopg.Connection]:
    with psycopg.connect(dsn, autocommit=True) as connection:
        yield connection


TIMELINE_EXTRA: dict[str, list[list[object]]] = {
    "olist_customers_dataset.csv": [
        ["c3", "u3", 13023, "campinas", "SP"],
        ["c4", "u4", 1310, "sao paulo", "SP"],
    ],
    "olist_orders_dataset.csv": [
        [
            "o3",
            "c3",
            "delivered",
            "2017-03-01 09:00:00",
            "",
            "2017-03-02 10:00:00",
            "2017-03-05 12:00:00",
            "2017-03-10 00:00:00",
        ],
        [
            "o4",
            "c4",
            "canceled",
            "2017-04-01 08:00:00",
            "2017-04-01 08:30:00",
            "",
            "",
            "2017-04-15 00:00:00",
        ],
    ],
    "olist_order_items_dataset.csv": [
        ["o3", 1, "p1", "s1", "2017-03-03 09:00:00", "12.50", "3.00"],
        ["o4", 1, "p2", "s1", "2017-04-03 08:00:00", "7.00", "2.00"],
    ],
    "olist_order_payments_dataset.csv": [
        ["o3", 1, "credit_card", 1, "15.50"],
        ["o4", 1, "boleto", 1, "9.00"],
    ],
    "olist_order_reviews_dataset.csv": [
        ["r3", "o3", 4, "", "ok", "2017-03-06 00:00:00", "2017-03-07 10:00:00"],
    ],
}


def _write_csvs(directory: Path, extra: dict[str, list[list[object]]]) -> Path:
    for filename, (header, rows) in FIXTURE.items():
        with (directory / filename).open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(header)
            writer.writerows([*rows, *extra.get(filename, [])])
    return directory


@pytest.fixture
def tmp_data_dir(tmp_path: Path) -> Path:
    return _write_csvs(tmp_path, TIMELINE_EXTRA)


@pytest.fixture
def tmp_csv_dir(tmp_path: Path) -> Path:
    return _write_csvs(tmp_path, {})
