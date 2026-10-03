import csv
import os
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest
from dotenv import find_dotenv, load_dotenv

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
            ["r2", "o2", 2, "", "", "2017-02-18 00:00:00", "2017-02-19 10:00:00"],
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


@pytest.fixture
def dsn() -> str:
    value = os.environ.get("POSTGRES_DSN")
    if not value:
        pytest.skip("POSTGRES_DSN is not set")
    try:
        psycopg.connect(value, connect_timeout=3).close()
    except psycopg.OperationalError:
        pytest.skip("Postgres is unreachable")
    return value


@pytest.fixture
def conn(dsn: str) -> Iterator[psycopg.Connection]:
    with psycopg.connect(dsn, autocommit=True) as connection:
        yield connection


@pytest.fixture
def tmp_csv_dir(tmp_path: Path) -> Path:
    for filename, (header, rows) in FIXTURE.items():
        with (tmp_path / filename).open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(header)
            writer.writerows(rows)
    return tmp_path
