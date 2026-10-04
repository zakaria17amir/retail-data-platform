"""`retail-ml forecast demand`: the champion's next-28-day forecast plus its test-window forecast
next to the actuals (`split = test`) for the Forecast vs Actual report."""

from __future__ import annotations

import mlflow
import numpy as np
import pandas as pd
from mlflow import MlflowClient

from retail_ml.forecast.features import HORIZON, KEYS, modelled_series, window
from retail_ml.forecast.train import DemandForecastConfig, model_inputs, read_demand
from retail_ml.late_delivery.promote import champion_version

OUTPUT_COLUMNS = [
    "date",
    *KEYS,
    "split",
    "forecast",
    "actual",
    "model_version",
    "run_ts",
]


def extend(df: pd.DataFrame, horizon: int = HORIZON) -> pd.DataFrame:
    """History plus `horizon` days after the last date for every series (orders NaN)."""
    end = df["date"].max()
    days = pd.date_range(end + pd.Timedelta(days=1), periods=horizon, freq="D")
    series = df[KEYS].drop_duplicates()
    future = series.merge(pd.DataFrame({"date": days}), how="cross").assign(orders=np.nan)
    return pd.concat([df, future[df.columns]], ignore_index=True)


def forecast_demand(
    cfg: DemandForecastConfig, client: MlflowClient, now: pd.Timestamp | None = None
) -> pd.DataFrame:
    name = cfg.registered_model
    version = champion_version(client, name)
    if version is None:
        raise RuntimeError(f"no champion for {name}: run `retail-ml train {name}` first")
    model = mlflow.pyfunc.load_model(f"models:/{name}/{version}")
    df, _ = modelled_series(read_demand(cfg.demand_path))
    df = df[["date", *KEYS, "orders"]].reset_index(drop=True)
    end = df["date"].max()
    cutoff = end - pd.Timedelta(days=HORIZON - 1)

    parts = []
    for split, frame, start in [
        ("test", window(df, cutoff), cutoff),
        ("forecast", extend(df), end + pd.Timedelta(days=1)),
    ]:
        p = np.asarray(model.predict(model_inputs(frame)), dtype="float64")
        rows = (frame["date"] >= start).to_numpy()
        actual = df["orders"].reindex(frame.index[rows]) if split == "test" else np.nan
        parts.append(
            frame.loc[rows, ["date", *KEYS]].assign(split=split, forecast=p[rows], actual=actual)
        )
    out = pd.concat(parts, ignore_index=True)
    out["date"] = out["date"].dt.date
    out["model_version"] = version
    out["run_ts"] = pd.Timestamp.now(tz="UTC") if now is None else now
    out = out[OUTPUT_COLUMNS]
    cfg.output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(cfg.output_path, index=False)
    return out
