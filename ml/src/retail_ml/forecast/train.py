"""Demand forecast: seasonal-naive baseline then one global LightGBM, 3-fold rolling-origin backtest
plus the last-28-day test window, MLflow runs, registry and champion/challenger promotion."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import lightgbm as lgb
import mlflow
import numpy as np
import pandas as pd
import yaml
from mlflow import MlflowClient
from mlflow.models import infer_signature
from mlflow.pyfunc.model import PythonModel

from retail_ml.config import check_promotion_mode, gold_dir
from retail_ml.data import sha256_file
from retail_ml.forecast.features import (
    HORIZON,
    KEYS,
    LAGS,
    ROLLING,
    demand_features,
    modelled_series,
    seasonal_naive,
    wape,
    window,
)
from retail_ml.late_delivery.promote import CHALLENGER, _champion_version, approve
from retail_ml.late_delivery.train import git_sha

DEFAULT_CONFIG = Path(__file__).resolve().parents[3] / "configs" / "demand_forecast.yaml"
MODEL_INPUT_COLUMNS = ["date", *KEYS, "orders"]
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DemandForecastConfig:
    experiment: str
    registered_model: str
    demand_path: Path
    output_path: Path
    backtest_folds: int
    top_series: int
    lightgbm: dict[str, Any]


def load_demand_forecast_config(path: Path | None = None) -> DemandForecastConfig:
    raw = yaml.safe_load((path or DEFAULT_CONFIG).read_text(encoding="utf-8"))
    return DemandForecastConfig(
        experiment=raw["experiment"],
        registered_model=raw["registered_model"],
        demand_path=gold_dir() / raw["demand_path"],
        output_path=gold_dir() / raw["output_path"],
        backtest_folds=int(raw["backtest_folds"]),
        top_series=int(raw["top_series"]),
        lightgbm=dict(raw["lightgbm"]),
    )


def read_demand(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"])
    df["orders"] = pd.to_numeric(df["orders"]).astype("float64")
    for key in KEYS:
        df[key] = df[key].astype(str)
    return df.reset_index(drop=True)


def model_inputs(df: pd.DataFrame) -> pd.DataFrame:
    """Model signature shape: naive dates, string ids, float orders (NaN = unknown/future)."""
    return df[MODEL_INPUT_COLUMNS].astype({"orders": "float64"})


class DemandModel(PythonModel):
    """Demand rows in (history + horizon rows with NaN orders), one forecast per row out."""

    def __init__(self, regressor: Any, vocabulary: dict[str, list[str]]) -> None:
        self.regressor, self.vocabulary = regressor, vocabulary

    def predict(self, context: Any, model_input: pd.DataFrame, params: Any = None) -> Any:
        X = demand_features(model_input.reset_index(drop=True), self.vocabulary)
        return np.asarray(self.regressor.predict(X), dtype="float64")


class SeasonalNaiveModel(PythonModel):
    """Baseline with the same interface: the horizon starts at the first NaN `orders` day."""

    def predict(self, context: Any, model_input: pd.DataFrame, params: Any = None) -> Any:
        df = model_input.reset_index(drop=True)
        missing = pd.to_datetime(df["date"])[df["orders"].isna()]
        if missing.empty:
            return np.full(len(df), np.nan)
        return seasonal_naive(df, missing.min()).to_numpy()


@dataclass(frozen=True)
class ForecastDecision:
    promoted: bool
    reason: str
    challenger_wape: float
    champion_wape: float | None


@dataclass(frozen=True)
class ForecastTrainResult:
    run_ids: dict[str, str]
    metrics: dict[str, dict[str, float]]
    version: str
    decision: ForecastDecision
    excluded: pd.DataFrame


def _horizon_forecast(
    predict: Callable[[pd.DataFrame], Any], frame: pd.DataFrame, actual: pd.Series
) -> np.ndarray:
    p = np.asarray(predict(model_inputs(frame)), dtype="float64")
    return p[frame.index.get_indexer(actual.index)]


def _load(name: str, version: str) -> Any:
    return mlflow.pyfunc.load_model(f"models:/{name}/{version}")


def forecast_test_failures(p: np.ndarray) -> list[str]:
    if np.isnan(p).any():
        return ["nan forecasts"]
    return ["negative forecasts"] if (p < 0).any() else []


def promote_forecast(
    client: MlflowClient,
    name: str,
    version: str,
    frame: pd.DataFrame,
    actual: pd.Series,
    mode: str = "auto",
) -> ForecastDecision:
    """Set `challenger`; in auto mode move `champion` when the forecasts have no NaN/negatives and
    WAPE beats the champion's, re-scored on the same window (`frame` = `window(...)`, `actual`
    indexed like its horizon rows)."""
    mode = check_promotion_mode(mode)
    client.set_registered_model_alias(name, CHALLENGER, version)
    p = _horizon_forecast(_load(name, version).predict, frame, actual)
    score = wape(actual, p)
    if failures := forecast_test_failures(p):
        return ForecastDecision(False, "model tests failed: " + "; ".join(failures), score, None)

    champion = _champion_version(client, name)
    champion_score = None
    reason = "no champion"
    if champion is not None:
        try:
            p_champion = _horizon_forecast(_load(name, champion).predict, frame, actual)
            champion_score = wape(actual, p_champion)
            reason = f"beats champion v{champion}"
        except Exception:
            logger.exception("champion v%s of %s could not be scored", champion, name)
        if champion_score is None or not np.isfinite(champion_score):
            champion_score, reason = None, "champion could not be scored"

    if champion_score is not None and not score < champion_score:
        reason = f"wape {score:.4f} >= champion v{champion} {champion_score:.4f}"
        return ForecastDecision(False, reason, score, champion_score)
    if mode == "manual":
        print(f"approve with: retail-ml promote {name} --version {version}")
        return ForecastDecision(
            False, f"manual mode: awaiting approval ({reason})", score, champion_score
        )
    approve(client, name, version)
    return ForecastDecision(True, reason, score, champion_score)


def _fit(cfg: DemandForecastConfig, df: pd.DataFrame, cutoff: pd.Timestamp) -> DemandModel:
    vocabulary = {key: sorted(df[key].unique()) for key in KEYS}
    history = df[df["date"] < cutoff].reset_index(drop=True)
    X = demand_features(history, vocabulary)
    rows = X[f"lag_{HORIZON}"].notna()
    regressor = lgb.LGBMRegressor(**cfg.lightgbm).fit(X[rows], history.loc[rows, "orders"])
    return DemandModel(regressor, vocabulary)


def _evaluate(model: PythonModel, df: pd.DataFrame, cutoff: pd.Timestamp) -> pd.DataFrame:
    """Horizon rows of the window at `cutoff` with `actual` and `forecast`."""
    frame = window(df, cutoff)
    horizon = frame.index[frame["date"] >= cutoff]
    actual = df.loc[horizon, "orders"]
    p = _horizon_forecast(lambda X: model.predict(None, X), frame, actual)
    return df.loc[horizon, ["date", *KEYS]].assign(actual=actual, forecast=p)


def _metrics(
    model: PythonModel,
    df: pd.DataFrame,
    folds: list[pd.Timestamp],
    cutoff: pd.Timestamp,
    top: list[tuple[str, str]],
    top_n: int,
    fold_model: Callable[[pd.Timestamp], PythonModel],
) -> dict[str, float]:
    m: dict[str, float] = {}
    for k, start in enumerate(folds, 1):
        scored = _evaluate(fold_model(start), df, start)
        m[f"backtest_wape_fold{k}"] = wape(scored["actual"], scored["forecast"])
    m["backtest_wape_mean"] = float(
        np.mean([m[f"backtest_wape_fold{k}"] for k in range(1, len(folds) + 1)])
    )
    test = _evaluate(model, df, cutoff)
    m["test_wape"] = wape(test["actual"], test["forecast"])
    in_top = pd.MultiIndex.from_frame(test[KEYS]).isin(top)
    m[f"test_wape_top{top_n}"] = wape(test.loc[in_top, "actual"], test.loc[in_top, "forecast"])
    for cat, state in top:
        s = test[(test["product_category"] == cat) & (test["customer_state"] == state)]
        m[f"test_wape.{cat}.{state}"] = wape(s["actual"], s["forecast"])
    return m


def train(cfg: DemandForecastConfig, promotion_mode: str = "auto") -> ForecastTrainResult:
    df, excluded = modelled_series(read_demand(cfg.demand_path))
    df = df.reset_index(drop=True)
    end = df["date"].max()
    cutoff = end - pd.Timedelta(days=HORIZON - 1)
    folds = [cutoff - pd.Timedelta(days=HORIZON * k) for k in range(cfg.backtest_folds, 0, -1)]
    volume = df[df["date"] < cutoff].groupby(KEYS)["orders"].sum().sort_values(ascending=False)
    top = [(str(c), str(s)) for c, s in volume.index[: cfg.top_series]]
    lineage = {"data_sha256": sha256_file(cfg.demand_path), "git_sha": git_sha()}
    common = {
        "horizon": HORIZON,
        "lags": ",".join(map(str, LAGS)),
        "rolling": ",".join(map(str, ROLLING)),
        "fold_starts": ",".join(f.date().isoformat() for f in folds),
        "test_start": cutoff.date().isoformat(),
        "test_end": end.date().isoformat(),
        "n_series_modelled": df.groupby(KEYS).ngroups,
        "n_series_excluded": len(excluded),
    }
    final = _fit(cfg, df, cutoff)
    candidates: list[tuple[str, PythonModel, dict[str, Any], Callable[..., PythonModel]]] = [
        ("seasonal_naive", SeasonalNaiveModel(), {}, lambda _: SeasonalNaiveModel()),
        ("lightgbm", final, cfg.lightgbm, lambda start: _fit(cfg, df, start)),
    ]

    mlflow.set_experiment(cfg.experiment)
    run_ids: dict[str, str] = {}
    metrics: dict[str, dict[str, float]] = {}
    model_uris: dict[str, str] = {}
    example = model_inputs(window(df, cutoff).head(5))
    for model_type, model, params, fold_model in candidates:  # baseline first
        scores = _metrics(model, df, folds, cutoff, top, cfg.top_series, fold_model)
        with mlflow.start_run(run_name=model_type) as run:
            mlflow.set_tags({"model_type": model_type, **lineage})
            mlflow.log_params({**common, **params})
            mlflow.log_metrics(scores)
            mlflow.log_text(excluded.to_csv(index=False), "excluded_series.csv")
            info = mlflow.pyfunc.log_model(
                name="model",
                python_model=model,
                signature=infer_signature(example, model.predict(None, example)),
                input_example=example,
            )
        run_ids[model_type], metrics[model_type] = run.info.run_id, scores
        model_uris[model_type] = info.model_uri

    version = str(mlflow.register_model(model_uris["lightgbm"], cfg.registered_model).version)
    frame = window(df, cutoff)
    actual = df.loc[frame.index[frame["date"] >= cutoff], "orders"]
    decision = promote_forecast(
        MlflowClient(), cfg.registered_model, version, frame, actual, mode=promotion_mode
    )
    return ForecastTrainResult(run_ids, metrics, version, decision, excluded)
