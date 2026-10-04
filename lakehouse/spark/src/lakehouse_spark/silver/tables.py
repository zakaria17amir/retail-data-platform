from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse_spark.cli import EVENT_TYPES
from lakehouse_spark.silver.domain.catalog import CATEGORIES_RULES, products_rules
from lakehouse_spark.silver.domain.events import event_rules
from lakehouse_spark.silver.domain.geo import GEO_RULES
from lakehouse_spark.silver.domain.sales import (
    ORDER_ITEMS_RULES,
    ORDERS_RULES,
    PAYMENTS_RULES,
    REVIEWS_RULES,
)
from lakehouse_spark.silver.rules import Rule

RulesFactory = Callable[[SparkSession, str], tuple[Rule, ...]]


@dataclass(frozen=True)
class TableSpec:
    name: str
    bronze: tuple[str, ...]
    mode: Literal["current", "scd2", "events"]
    key: tuple[str, ...]
    rules: RulesFactory
    tracked: tuple[str, ...] = ()


def read_if_exists(spark: SparkSession, path: str) -> DataFrame | None:
    if not DeltaTable.isDeltaTable(spark, path):
        return None
    return spark.read.format("delta").load(path)


def _fixed(rules: tuple[Rule, ...]) -> RulesFactory:
    return lambda spark, silver: rules


def _products(spark: SparkSession, silver: str) -> tuple[Rule, ...]:
    categories = read_if_exists(spark, f"{silver}/catalog/categories")
    if categories is not None:
        categories = categories.filter(~F.col("_is_deleted")).dropDuplicates(
            ["product_category_name"]
        )
    return products_rules(categories)


def _events(spark: SparkSession, silver: str) -> tuple[Rule, ...]:
    return event_rules(read_if_exists(spark, f"{silver}/events/clickstream"))


TABLES: tuple[TableSpec, ...] = (
    TableSpec(
        "catalog/categories",
        ("olist/product_category_name_translation",),
        "current",
        ("product_category_name",),
        _fixed(CATEGORIES_RULES),
    ),
    TableSpec(
        "catalog/products",
        ("olist/products",),
        "scd2",
        ("product_id",),
        _products,
        (
            "product_category_name",
            "product_category_name_english",
            "product_name_length",
            "product_description_length",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ),
    ),
    TableSpec(
        "party/customers",
        ("olist/customers",),
        "scd2",
        ("customer_id",),
        _fixed(()),
        ("customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"),
    ),
    TableSpec(
        "party/sellers",
        ("olist/sellers",),
        "scd2",
        ("seller_id",),
        _fixed(()),
        ("seller_zip_code_prefix", "seller_city", "seller_state"),
    ),
    TableSpec(
        "geo/geolocation_points",
        ("olist/geolocation",),
        "current",
        ("geolocation_pk",),
        _fixed(GEO_RULES),
    ),
    TableSpec("sales/orders", ("olist/orders",), "current", ("order_id",), _fixed(ORDERS_RULES)),
    TableSpec(
        "sales/order_items",
        ("olist/order_items",),
        "current",
        ("order_id", "order_item_id"),
        _fixed(ORDER_ITEMS_RULES),
    ),
    TableSpec(
        "sales/order_payments",
        ("olist/order_payments",),
        "current",
        ("order_id", "payment_sequential"),
        _fixed(PAYMENTS_RULES),
    ),
    TableSpec(
        "sales/order_reviews",
        ("olist/order_reviews",),
        "current",
        ("review_pk",),
        _fixed(REVIEWS_RULES),
    ),
    TableSpec(
        "events/clickstream",
        tuple(f"events/{t}" for t in EVENT_TYPES),
        "events",
        ("event_id",),
        _events,
    ),
)
