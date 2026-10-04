"""Feast definitions: daily seller snapshots built in dbt (deliveries before feature_ts only), and
the streaming session / product-popularity views pushed by the Spark realtime app."""

import os
from datetime import timedelta
from pathlib import Path

from feast import Entity, FeatureView, Field, FileSource, PushSource, ValueType
from feast.types import Float64, Int64, String, UnixTimestamp

GOLD_DIR = Path(os.environ.get("GOLD_DIR") or "data/gold").resolve()

seller = Entity(name="seller", join_keys=["seller_id"], value_type=ValueType.STRING)

seller_features_daily = FileSource(
    name="seller_features_daily",
    path=(GOLD_DIR / "ml" / "seller_features_daily.parquet").as_posix(),
    timestamp_field="feature_ts",
    created_timestamp_column="created_ts",
)

seller_stats = FeatureView(
    name="seller_stats",
    entities=[seller],
    ttl=timedelta(days=365),
    schema=[
        Field(name="seller_orders_90d", dtype=Int64),
        Field(name="seller_late_rate_90d", dtype=Float64),
        Field(name="seller_avg_delivery_days_90d", dtype=Float64),
    ],
    source=seller_features_daily,
    online=True,
)

session = Entity(name="session", join_keys=["session_id"], value_type=ValueType.STRING)
product = Entity(name="product", join_keys=["product_id"], value_type=ValueType.STRING)

# Online-only: rows arrive via the feature server's /push. Feast requires a batch source on a
# PushSource; nothing writes these files (offline training recomputes from sessions_offline).
session_push = PushSource(
    name="session_push",
    batch_source=FileSource(
        name="session_features_batch",
        path=(GOLD_DIR / "ml" / "push" / "session_features.parquet").as_posix(),
        timestamp_field="event_ts",
    ),
)

popularity_push = PushSource(
    name="popularity_push",
    batch_source=FileSource(
        name="product_popularity_batch",
        path=(GOLD_DIR / "ml" / "push" / "product_popularity.parquet").as_posix(),
        timestamp_field="event_ts",
    ),
)

session_features = FeatureView(
    name="session_features",
    entities=[session],
    ttl=timedelta(days=1),
    schema=[
        Field(name="n_events", dtype=Int64),
        Field(name="n_product_views", dtype=Int64),
        Field(name="n_categories", dtype=Int64),
        Field(name="last_category", dtype=String),
        Field(name="last_product_ids", dtype=String),
        Field(name="n_cart_adds", dtype=Int64),
        Field(name="dwell_seconds", dtype=Float64),
        Field(name="session_start_ts", dtype=UnixTimestamp),
    ],
    source=session_push,
    online=True,
)

product_popularity = FeatureView(
    name="product_popularity",
    entities=[product],
    ttl=timedelta(days=7),
    schema=[
        Field(name="views_1h", dtype=Int64),
        Field(name="views_24h", dtype=Int64),
        Field(name="carts_24h", dtype=Int64),
    ],
    source=popularity_push,
    online=True,
)
