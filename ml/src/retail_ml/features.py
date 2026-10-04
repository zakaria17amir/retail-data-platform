"""Order-level features shared by training, batch scoring and the API (no training/serving skew)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Self

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin

UNKNOWN = "__unknown__"
CATEGORICAL = ["payment_type", "product_category", "customer_state", "seller_state"]
ORDER_FEATURES = [
    "freight_ratio",
    "distance_km",
    "n_items",
    "n_sellers",
    "estimated_days",
    "approve_hour",
    "approve_dow",
    "approve_month",
    "payment_type",
    "payment_installments",
    "product_category",
    "customer_state",
    "seller_state",
    "same_state",
]
NUMERIC = [c for c in ORDER_FEATURES if c not in CATEGORICAL]
SELLER_FEATURES = ["seller_orders_90d", "seller_late_rate_90d", "seller_avg_delivery_days_90d"]

EARTH_RADIUS_KM = 6371.0088


def haversine_km(lat1: Any, lng1: Any, lat2: Any, lng2: Any) -> Any:
    """Great-circle distance in km; works on scalars and arrays, NaN in → NaN out."""
    p1, p2 = np.radians(lat1), np.radians(lat2)
    dlat, dlng = p2 - p1, np.radians(lng2) - np.radians(lng1)
    a = np.sin(dlat / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dlng / 2) ** 2
    return 2 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(a))


def _num(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_numeric(df[col], errors="coerce").astype("float64")


def _categorical(values: pd.Series, vocabulary: Sequence[str] | None) -> pd.Series:
    s = values.astype("object").where(values.notna(), UNKNOWN).astype(str)
    levels = sorted(set(s) - {UNKNOWN}) if vocabulary is None else list(vocabulary)
    if vocabulary is not None:
        s = s.where(s.isin(levels), UNKNOWN)
    return pd.Series(pd.Categorical(s, categories=[*levels, UNKNOWN]), index=values.index)


def order_features(
    df: pd.DataFrame, vocabulary: Mapping[str, Sequence[str]] | None = None
) -> pd.DataFrame:
    """Contract features from raw order columns. Pure: no I/O, input untouched.

    `vocabulary` fixes the category levels (values outside it become `UNKNOWN`);
    without it the levels are the observed values. `UNKNOWN` is always a level.
    """
    approved = pd.to_datetime(df["order_approved_ts_utc"], utc=True)
    estimated = pd.to_datetime(df["order_estimated_delivery_ts_utc"], utc=True)
    price, freight = _num(df, "total_price"), _num(df, "total_freight")
    out = pd.DataFrame(index=df.index)
    out["freight_ratio"] = freight / price.where(price != 0)
    out["distance_km"] = haversine_km(
        _num(df, "customer_lat"),
        _num(df, "customer_lng"),
        _num(df, "seller_lat"),
        _num(df, "seller_lng"),
    )
    out["n_items"] = _num(df, "n_items")
    out["n_sellers"] = _num(df, "n_sellers")
    out["estimated_days"] = (estimated - approved).dt.total_seconds() / 86400
    out["approve_hour"] = approved.dt.hour
    out["approve_dow"] = approved.dt.dayofweek
    out["approve_month"] = approved.dt.month
    out["payment_installments"] = _num(df, "payment_installments")
    for col in CATEGORICAL:
        out[col] = _categorical(df[col], None if vocabulary is None else vocabulary[col])
    same = df["customer_state"].notna() & (df["customer_state"] == df["seller_state"])
    out["same_state"] = same.astype("int64")
    return out[ORDER_FEATURES]


class OrderFeatures(TransformerMixin, BaseEstimator):  # type: ignore[misc]
    """Pipeline step: learns the category vocabulary at fit, then `order_features` + seller columns.

    Seller features missing from the input (unseen seller, no Feast row) become NaN.
    """

    def fit(self, X: pd.DataFrame, y: Any = None) -> Self:
        self.vocabulary_ = {
            col: sorted(set(X[col].dropna().astype(str)) - {UNKNOWN}) for col in CATEGORICAL
        }
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        out = order_features(X, self.vocabulary_)
        for col in SELLER_FEATURES:
            out[col] = _num(X, col) if col in X else np.nan
        return out
