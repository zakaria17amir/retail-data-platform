from pathlib import Path

import polars as pl

from replayer.schema import TABLES, Table

GEOLOCATION_CAP = 1000


def _read(src: Path, table: Table) -> pl.DataFrame:
    return pl.read_csv(src / table.csv_file, infer_schema=False)


def sample(src: Path, dest: Path, n_orders: int, seed: int = 42) -> dict[str, int]:
    frames = {t.name: _read(src, t) for t in TABLES}

    orders = frames["orders"]
    if n_orders < orders.height:
        chosen = orders.sort("order_id").sample(n=n_orders, seed=seed)["order_id"]
        orders = orders.filter(pl.col("order_id").is_in(chosen.implode()))

    order_ids = orders["order_id"]
    items = frames["order_items"].filter(pl.col("order_id").is_in(order_ids.implode()))
    payments = frames["order_payments"].filter(pl.col("order_id").is_in(order_ids.implode()))
    reviews = frames["order_reviews"].filter(pl.col("order_id").is_in(order_ids.implode()))
    customers = frames["customers"].filter(
        pl.col("customer_id").is_in(orders["customer_id"].implode())
    )
    products = frames["products"].filter(pl.col("product_id").is_in(items["product_id"].implode()))
    sellers = frames["sellers"].filter(pl.col("seller_id").is_in(items["seller_id"].implode()))
    translation = frames["product_category_name_translation"].filter(
        pl.col("product_category_name").is_in(products["product_category_name"].implode())
    )
    zips = pl.concat([customers["customer_zip_code_prefix"], sellers["seller_zip_code_prefix"]])
    geolocation = (
        frames["geolocation"]
        .filter(pl.col("geolocation_zip_code_prefix").is_in(zips.implode()))
        .sort(pl.col("geolocation_zip_code_prefix").cast(pl.Int64), maintain_order=True)
        .head(GEOLOCATION_CAP)
    )

    result = {
        "product_category_name_translation": translation,
        "customers": customers,
        "sellers": sellers,
        "products": products,
        "geolocation": geolocation,
        "orders": orders,
        "order_items": items,
        "order_payments": payments,
        "order_reviews": reviews,
    }
    dest.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}
    for t in TABLES:
        result[t.name].select(t.columns).write_csv(dest / t.csv_file)
        counts[t.name] = result[t.name].height
    return counts
