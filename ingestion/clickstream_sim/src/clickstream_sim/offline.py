import random
from dataclasses import fields
from pathlib import Path

import polars as pl

from clickstream_sim.events import Catalogue, Event, OrderRef, order_sessions

TRAINING = Path("ml/late_delivery_training.parquet")
PRODUCTS = Path("dim_product.parquet")
ITEMS = Path("fct_order_items.parquet")
EVENT_FIELDS = tuple(f.name for f in fields(Event))
SCHEMA: dict[str, type[pl.DataType]] = {
    **dict.fromkeys(EVENT_FIELDS, pl.String),
    "quantity": pl.Int32,
    "rank": pl.Int32,
    "is_purchase_target": pl.Boolean,
}


def orders_from_gold(training: pl.DataFrame, items: pl.DataFrame) -> tuple[list[OrderRef], int]:
    products = (
        items.sort("order_id", "order_item_id")
        .group_by("order_id", maintain_order=True)
        .agg(pl.col("product_id").unique(maintain_order=True))
    )
    joined = (
        training.select(
            "order_id",
            "customer_id",
            pl.col("order_purchase_ts_utc").dt.convert_time_zone("UTC").dt.replace_time_zone(None),
        )
        .join(products, on="order_id", how="left")
        .sort("order_purchase_ts_utc", "order_id")
    )
    orders = [
        OrderRef(order_id, customer_id, purchase_ts, tuple(product_ids))
        for order_id, customer_id, purchase_ts, product_ids in joined.iter_rows()
        if product_ids
    ]
    return orders, joined.height - len(orders)


def catalogue_from_products(products: pl.DataFrame) -> Catalogue:
    current = products.filter("is_current").select("product_id", "product_category_name")
    return Catalogue.from_rows(current.iter_rows())


def history(
    orders: list[OrderRef], catalogue: Catalogue, *, seed: int, browsing_ratio: int
) -> pl.DataFrame:
    rng = random.Random(seed)
    columns: dict[str, list[object]] = {name: [] for name in SCHEMA}
    for order in orders:
        purchased = set(order.product_ids)
        for i, session in enumerate(order_sessions(order, catalogue, rng, browsing_ratio)):
            for event in session:
                for name in EVENT_FIELDS:
                    columns[name].append(getattr(event, name))
                columns["is_purchase_target"].append(i == 0 and event.product_id in purchased)
    return pl.DataFrame(columns, schema=SCHEMA)


def generate(gold_dir: Path, out: Path, *, seed: int, browsing_ratio: int) -> dict[str, int]:
    orders, without_items = orders_from_gold(
        pl.read_parquet(gold_dir / TRAINING), pl.read_parquet(gold_dir / ITEMS)
    )
    catalogue = catalogue_from_products(pl.read_parquet(gold_dir / PRODUCTS))
    frame = history(orders, catalogue, seed=seed, browsing_ratio=browsing_ratio)
    out.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(out)
    checkouts = frame.filter(pl.col("event_type") == "checkout_started")["session_id"]
    is_converting = pl.col("session_id").is_in(checkouts.implode())
    converting, browsing = frame.filter(is_converting), frame.filter(~is_converting)
    return {
        "orders": len(orders),
        "orders_without_items": without_items,
        "sessions_converting": converting["session_id"].n_unique(),
        "sessions_browsing": browsing["session_id"].n_unique(),
        "rows": frame.height,
        "rows_converting": converting.height,
        "rows_browsing": browsing.height,
        "purchase_targets": int(frame["is_purchase_target"].sum()),
    }
