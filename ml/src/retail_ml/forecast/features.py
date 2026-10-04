"""Demand features with lags >= the 28-day horizon only, so one model forecasts all 28 days directly
from history that is known at the cutoff. Pure functions over the `demand_daily` contract."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np
import pandas as pd

HORIZON = 28
LAGS = (28, 35, 42, 56)
ROLLING = (7, 28)
KEYS = ["product_category", "customer_state"]
FEATURES = [
    *(f"lag_{k}" for k in LAGS),
    *(f"rmean_{w}_lag_{HORIZON}" for w in ROLLING),
    "dow",
    "month",
    "day",
    *KEYS,
]


def _lookup(df: pd.DataFrame, source_dates: pd.Series, values: Any) -> np.ndarray:
    """Per row: `values` of the same series on `source_dates` (NaN where that day is absent)."""
    keys = df[KEYS].astype(str)
    left = keys.assign(date=source_dates.to_numpy())
    right = keys.assign(date=pd.to_datetime(df["date"]).to_numpy(), _value=np.asarray(values))
    merged = left.merge(right, on=[*KEYS, "date"], how="left")
    return merged["_value"].to_numpy(dtype="float64")


def demand_features(df: pd.DataFrame, vocabulary: Mapping[str, Sequence[str]]) -> pd.DataFrame:
    """Features per row (date, product_category, customer_state, orders; one row per series-day).

    Row d only reads `orders` dated <= d - 28. Series ids outside `vocabulary` become NaN."""
    date = pd.to_datetime(df["date"])
    orders = pd.to_numeric(df["orders"], errors="coerce").astype("float64")
    out = pd.DataFrame(index=df.index)
    for lag in LAGS:
        out[f"lag_{lag}"] = _lookup(df, date - pd.Timedelta(days=lag), orders)
    frame = df[KEYS].astype(str).assign(date=date, orders=orders).sort_values([*KEYS, "date"])
    for w in ROLLING:
        rolled = pd.Series(np.nan, index=frame.index)
        for _, g in frame.groupby(KEYS, sort=False):
            mean = g.rolling(f"{w}D", on="date", min_periods=1)["orders"].mean()
            rolled.loc[g.index] = mean.to_numpy()
        out[f"rmean_{w}_lag_{HORIZON}"] = _lookup(
            df, date - pd.Timedelta(days=HORIZON), rolled.reindex(df.index)
        )
    out["dow"] = date.dt.dayofweek
    out["month"] = date.dt.month
    out["day"] = date.dt.day
    for key in KEYS:
        out[key] = pd.Categorical(df[key].astype(str), categories=list(vocabulary[key]))
    return out[FEATURES]


def seasonal_naive(df: pd.DataFrame, cutoff: pd.Timestamp) -> pd.Series:
    """Baseline for days >= cutoff: y[t - 7 * ceil(h / 7)], h = days since cutoff + 1, i.e. the last
    observed week repeated. Only reads days < cutoff; NaN for rows before the cutoff."""
    date = pd.to_datetime(df["date"])
    h = (date - cutoff).dt.days + 1
    source = date - pd.to_timedelta(7 * np.ceil(h / 7), unit="D")
    observed = pd.to_numeric(df["orders"], errors="coerce").where(date < cutoff)
    value = _lookup(df, source, observed)
    return pd.Series(np.where(h >= 1, value, np.nan), index=df.index)


def window(df: pd.DataFrame, cutoff: pd.Timestamp, horizon: int = HORIZON) -> pd.DataFrame:
    """History before `cutoff` plus the horizon rows with `orders` hidden (NaN)."""
    date = pd.to_datetime(df["date"])
    out = df[date < cutoff + pd.Timedelta(days=horizon)].copy()
    out["orders"] = pd.to_numeric(out["orders"]).astype("float64")
    out.loc[date[out.index] >= cutoff, "orders"] = np.nan
    return out


def wape(actual: Any, forecast: Any) -> float:
    a, f = np.asarray(actual, dtype=float), np.asarray(forecast, dtype=float)
    total = a.sum()
    return float(np.abs(a - f).sum() / total) if total > 0 else float("nan")


def modelled_series(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Rows of `is_modelled` series, and one summary row per excluded series."""
    keep = df["is_modelled"].astype(bool)
    excluded = (
        df[~keep]
        .groupby(KEYS, as_index=False)
        .agg(orders=("orders", "sum"), days=("orders", "size"))
        .astype({"orders": "float64"})
    )
    return df[keep].drop(columns="is_modelled"), excluded
