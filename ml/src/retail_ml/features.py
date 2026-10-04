"""Order-level features shared by training, batch scoring and the API (no training/serving skew)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from functools import cache
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


def _num(df: pd.DataFrame, col: str) -> np.ndarray[Any, np.dtype[np.float64]]:
    return pd.to_numeric(df[col], errors="coerce").to_numpy(dtype="float64", na_value=np.nan)


def _strings(values: pd.Series) -> tuple[pd.Index, np.ndarray[Any, Any]]:
    """(values as str, null mask) without Series ops, so one-row frames stay cheap."""
    raw = values.to_numpy(dtype=object)
    return pd.Index(raw.astype(str)), pd.isna(raw)


@cache
def _dtype(levels: tuple[str, ...]) -> pd.CategoricalDtype:
    return pd.CategoricalDtype([*levels, UNKNOWN])  # cached: its hash table is built once


def _categorical(
    strings: pd.Index, null: np.ndarray[Any, Any], vocabulary: Sequence[str] | None
) -> pd.Categorical:
    levels = sorted(set(strings[~null]) - {UNKNOWN}) if vocabulary is None else vocabulary
    dtype = _dtype(tuple(levels))
    codes = dtype.categories.get_indexer(strings)
    codes[null | (codes < 0)] = len(levels)
    return pd.Categorical.from_codes(codes, dtype=dtype)  # type: ignore[arg-type]


def order_features(
    df: pd.DataFrame, vocabulary: Mapping[str, Sequence[str]] | None = None
) -> pd.DataFrame:
    """Contract features from raw order columns. Pure: no I/O, input untouched.

    `vocabulary` fixes the category levels (values outside it become `UNKNOWN`);
    without it the levels are the observed values. `UNKNOWN` is always a level.
    """
    approved = pd.DatetimeIndex(pd.to_datetime(df["order_approved_ts_utc"], utc=True))
    estimated = pd.DatetimeIndex(pd.to_datetime(df["order_estimated_delivery_ts_utc"], utc=True))
    price, freight = _num(df, "total_price"), _num(df, "total_freight")
    strings = {col: _strings(df[col]) for col in CATEGORICAL}
    (customer, customer_null), (seller, seller_null) = (
        strings["customer_state"],
        strings["seller_state"],
    )
    out = {
        "freight_ratio": freight / np.where(price != 0, price, np.nan),
        "distance_km": haversine_km(
            _num(df, "customer_lat"),
            _num(df, "customer_lng"),
            _num(df, "seller_lat"),
            _num(df, "seller_lng"),
        ),
        "n_items": _num(df, "n_items"),
        "n_sellers": _num(df, "n_sellers"),
        "estimated_days": (estimated - approved).total_seconds().to_numpy() / 86400,
        "approve_hour": approved.hour.to_numpy(),
        "approve_dow": approved.dayofweek.to_numpy(),
        "approve_month": approved.month.to_numpy(),
        "payment_installments": _num(df, "payment_installments"),
        **{
            col: _categorical(*strings[col], None if vocabulary is None else vocabulary[col])
            for col in CATEGORICAL
        },
        "same_state": (~customer_null & ~seller_null & (customer == seller)).astype("int64"),
    }
    return pd.DataFrame({col: out[col] for col in ORDER_FEATURES}, index=df.index)


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
