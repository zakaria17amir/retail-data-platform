"""Feast definitions: daily seller snapshots built in dbt (deliveries before feature_ts only)."""

import os
from datetime import timedelta
from pathlib import Path

from feast import Entity, FeatureView, Field, FileSource, ValueType
from feast.types import Float64, Int64

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
