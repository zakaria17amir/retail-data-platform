"""`retail-ml score late_delivery`: open orders scored by the registry champion.

Seller features are the Feast offline snapshot as of each order's approval (the same point-in-time
join as training), not wall-clock now: this dataset is historical, so "now" is past every snapshot
and would hand every open order the seller's latest (post-approval) stats."""

from __future__ import annotations

from pathlib import Path

import mlflow
import numpy as np
import pandas as pd
from feast import FeatureStore
from mlflow import MlflowClient

from retail_ml.data import historical_seller_features
from retail_ml.features import SELLER_FEATURES
from retail_ml.late_delivery.promote import _champion_version
from retail_ml.late_delivery.train import model_inputs

CLOSED_STATUSES = ("canceled", "unavailable")
OUTPUT_COLUMNS = ["order_id", "probability", "model_version", "scored_ts"]


def open_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Approved (every contract row is), not delivered, not canceled/unavailable."""
    is_open = df["order_delivered_customer_ts_utc"].isna() & ~df["order_status"].isin(
        CLOSED_STATUSES
    )
    return df[is_open].reset_index(drop=True)


def score(
    store: FeatureStore,
    client: MlflowClient,
    training_path: Path,
    name: str = "late_delivery",
    now: pd.Timestamp | None = None,
) -> pd.DataFrame:
    version = _champion_version(client, name)
    if version is None:
        raise RuntimeError(f"no champion for {name}: run `retail-ml train {name}` first")
    scored_ts = pd.Timestamp.now(tz="UTC") if now is None else now
    orders = open_orders(pd.read_parquet(training_path))
    if orders.empty:
        p = np.empty(0, dtype="float64")
    else:
        seller = historical_seller_features(store, orders)
        X = model_inputs(pd.concat([orders, seller[SELLER_FEATURES]], axis=1))
        model = mlflow.pyfunc.load_model(f"models:/{name}/{version}")
        p = np.asarray(model.predict(X), dtype="float64")
    return pd.DataFrame(
        {
            "order_id": orders["order_id"].to_numpy(),
            "probability": p,
            "model_version": version,
            "scored_ts": pd.Series([scored_ts] * len(orders), dtype="datetime64[ns, UTC]"),
        }
    )[OUTPUT_COLUMNS]
