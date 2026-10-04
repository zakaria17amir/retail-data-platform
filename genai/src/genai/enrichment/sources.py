"""Silver inputs for enrichment: current products, category translation, top reviews."""

from __future__ import annotations

import hashlib
import os
from collections.abc import Mapping, Sequence
from typing import Any

import pandas as pd
from deltalake import DeltaTable

from genai.enrichment.prompt import ProductContext

TOP_REVIEWS = 3


def storage_options(root: str, env: Mapping[str, str] = os.environ) -> dict[str, str]:
    if not root.startswith("s3"):
        return {}
    return {
        "AWS_ENDPOINT_URL": env.get("MINIO_ENDPOINT") or "http://127.0.0.1:9000",
        "AWS_ACCESS_KEY_ID": env.get("MINIO_ROOT_USER", "minio"),
        "AWS_SECRET_ACCESS_KEY": env.get("MINIO_ROOT_PASSWORD", "minio12345"),
        "AWS_ALLOW_HTTP": "true",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


def _read(root: str, path: str, columns: list[str]) -> pd.DataFrame:
    table = DeltaTable(f"{root}/silver/{path}", storage_options=storage_options(root))
    present = {f.name for f in table.schema().fields}
    df = table.to_pandas(
        columns=[c for c in [*columns, "is_current", "_is_deleted"] if c in present]
    )
    if "is_current" in df:
        df = df[df["is_current"].eq(True)]
    if "_is_deleted" in df:
        df = df[~df["_is_deleted"].eq(True)]
    return df[columns]


def review_texts(root: str) -> pd.DataFrame:
    """Distinct non-empty `title. message` per product, longest first: columns product_id, text."""
    reviews = _read(
        root, "sales/order_reviews", ["order_id", "review_comment_title", "review_comment_message"]
    )
    title = reviews["review_comment_title"].fillna("").str.strip()
    message = reviews["review_comment_message"].fillna("").str.strip()
    reviews = reviews.assign(text=(title.where(title.eq(""), title + ". ") + message).str.strip())
    reviews = reviews[reviews["text"].ne("")]
    items = _read(root, "sales/order_items", ["order_id", "product_id"]).drop_duplicates()
    joined = items.merge(reviews[["order_id", "text"]], on="order_id")
    joined = joined.drop_duplicates(["product_id", "text"])
    joined = joined.assign(length=joined["text"].str.len()).sort_values(
        ["product_id", "length", "text"], ascending=[True, False, True]
    )
    return joined[["product_id", "text"]]


def _top_reviews(root: str) -> dict[str, tuple[str, ...]]:
    top = review_texts(root).groupby("product_id").head(TOP_REVIEWS)
    return {str(pid): tuple(g["text"]) for pid, g in top.groupby("product_id")}


def _opt(value: Any) -> float | None:
    return None if pd.isna(value) else float(value)


def _str(value: Any) -> str | None:
    return None if pd.isna(value) else str(value)


def load_contexts(root: str) -> list[ProductContext]:
    products = _read(
        root,
        "catalog/products",
        [
            "product_id",
            "product_category_name",
            "product_category_name_english",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ],
    )
    categories = _read(
        root, "catalog/categories", ["product_category_name", "product_category_name_english"]
    ).drop_duplicates("product_category_name")
    products = products.merge(
        categories, on="product_category_name", how="left", suffixes=("", "_lookup")
    )
    reviews = _top_reviews(root)
    contexts = []
    for row in products.to_dict("records"):
        english = _str(row["product_category_name_english_lookup"]) or _str(
            row["product_category_name_english"]
        )
        photos = _opt(row["product_photos_qty"])
        contexts.append(
            ProductContext(
                product_id=str(row["product_id"]),
                category_pt=_str(row["product_category_name"]),
                category_en=english,
                weight_g=_opt(row["product_weight_g"]),
                length_cm=_opt(row["product_length_cm"]),
                height_cm=_opt(row["product_height_cm"]),
                width_cm=_opt(row["product_width_cm"]),
                photos=None if photos is None else int(photos),
                reviews=reviews.get(str(row["product_id"]), ()),
            )
        )
    return contexts


def stratified(
    contexts: Sequence[ProductContext], limit: int | None, category: str | None = None
) -> list[ProductContext]:
    """Deterministic round-robin across categories, products in hash order within each."""
    pool = [c for c in contexts if category is None or category in (c.category_en, c.category_pt)]
    pool.sort(key=lambda c: hashlib.sha256(c.product_id.encode()).hexdigest())
    seen: dict[str | None, int] = {}
    ranked = []
    for c in pool:
        rank = seen[c.category_en] = seen.get(c.category_en, -1) + 1
        ranked.append((rank, c.category_en or "", c))
    ranked.sort(key=lambda r: (r[0], r[1]))
    picked = [c for _, _, c in ranked]
    return picked if limit is None else picked[:limit]
