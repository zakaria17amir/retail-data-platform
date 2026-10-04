"""Late delivery: Feast PIT join, time split, prior + logistic + LightGBM, MLflow, promotion."""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from typing import Any

import lightgbm as lgb
import mlflow
import pandas as pd
from feast import FeatureStore
from mlflow import MlflowClient
from mlflow.models import infer_signature
from mlflow.pyfunc.model import PythonModel
from sklearn.compose import ColumnTransformer
from sklearn.dummy import DummyClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from retail_ml.config import LateDeliveryConfig
from retail_ml.data import frame_sha256, historical_seller_features, read_training, sha256_file
from retail_ml.features import CATEGORICAL, NUMERIC, SELLER_FEATURES, OrderFeatures
from retail_ml.late_delivery.evaluate import brier_baseline, classification_metrics
from retail_ml.late_delivery.promote import Decision, promote

LABEL = "is_late"
TIMESTAMPS = ["order_approved_ts_utc", "order_estimated_delivery_ts_utc"]
MODEL_INPUT_COLUMNS = [
    *TIMESTAMPS,
    "n_items",
    "n_sellers",
    "total_price",
    "total_freight",
    "product_category",
    "payment_type",
    "payment_installments",
    "customer_state",
    "customer_lat",
    "customer_lng",
    "seller_state",
    "seller_lat",
    "seller_lng",
    *SELLER_FEATURES,
]


class ProbabilityModel(PythonModel):
    """One artefact for batch and online: raw contract columns + seller features → P(late)."""

    def __init__(self, pipeline: Pipeline) -> None:
        self.pipeline = pipeline

    def predict(self, context: Any, model_input: pd.DataFrame, params: Any = None) -> Any:
        return self.pipeline.predict_proba(model_input)[:, 1]


@dataclass(frozen=True)
class TrainResult:
    run_ids: dict[str, str]
    metrics: dict[str, dict[str, float]]
    version: str
    decision: Decision


def git_sha() -> str:
    if sha := os.environ.get("GIT_SHA"):
        return sha
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True, timeout=10
        )
        return out.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return "unknown"


def model_inputs(df: pd.DataFrame) -> pd.DataFrame:
    """Model signature dtypes: numerics float (ints and NaN both fit), timestamps naive UTC
    (MLflow schemas reject tz-aware datetimes). Callers (scoring, API) shape inputs with this."""
    X = df[MODEL_INPUT_COLUMNS].copy()
    for col in X.columns.difference([*TIMESTAMPS, *CATEGORICAL]):
        X[col] = pd.to_numeric(X[col], errors="coerce").astype("float64")
    for col in TIMESTAMPS:
        X[col] = pd.to_datetime(X[col], utc=True).dt.tz_localize(None)
    for col in CATEGORICAL:
        X[col] = X[col].astype("object")
    return X


def split(df: pd.DataFrame, cfg: LateDeliveryConfig) -> dict[str, pd.DataFrame]:
    approved = pd.to_datetime(df["order_approved_ts_utc"], utc=True)
    bounds = {
        "train": approved < cfg.validation_start,
        "val": (approved >= cfg.validation_start) & (approved < cfg.test_start),
        "test": (approved >= cfg.test_start) & (approved < cfg.test_end),
    }
    return {name: df[mask].reset_index(drop=True) for name, mask in bounds.items()}


def _logistic(cfg: LateDeliveryConfig) -> Pipeline:
    prep = ColumnTransformer(
        [
            (
                "num",
                Pipeline(
                    [("impute", SimpleImputer(strategy="median")), ("scale", StandardScaler())]
                ),
                NUMERIC + SELLER_FEATURES,
            ),
            ("cat", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL),
        ]
    )
    return Pipeline(
        [
            ("features", OrderFeatures()),
            ("prep", prep),
            ("model", LogisticRegression(**cfg.logistic)),
        ]
    )


def _lightgbm(
    cfg: LateDeliveryConfig, X: dict[str, pd.DataFrame], y: dict[str, pd.Series]
) -> tuple[Pipeline, int]:
    """Native categoricals; early stopping on validation log loss (keeps the Brier gate honest)."""
    features = OrderFeatures().fit(X["train"])
    clf = lgb.LGBMClassifier(**cfg.lightgbm)
    clf.fit(
        features.transform(X["train"]),
        y["train"],
        eval_X=(features.transform(X["val"]),),
        eval_y=(y["val"],),
        callbacks=[lgb.early_stopping(cfg.early_stopping_rounds, verbose=False)],
    )
    return Pipeline([("features", features), ("model", clf)]), int(clf.best_iteration_)


def train(
    cfg: LateDeliveryConfig, store: FeatureStore, promotion_mode: str = "auto"
) -> TrainResult:
    df = read_training(cfg.training_path)
    seller = historical_seller_features(store, df)
    df = pd.concat([df, seller[SELLER_FEATURES]], axis=1)
    parts = split(df, cfg)
    X = {name: model_inputs(part) for name, part in parts.items()}
    y = {name: part[LABEL].astype(int) for name, part in parts.items()}
    lineage = {
        "data_sha256": sha256_file(cfg.training_path),
        "features_sha256": frame_sha256(seller, "order_id"),
        "git_sha": git_sha(),
    }
    common = {
        **{f"n_{name}": len(part) for name, part in parts.items()},
        "validation_start": cfg.validation_start.date().isoformat(),
        "test_start": cfg.test_start.date().isoformat(),
        "test_end": cfg.test_end.date().isoformat(),
    }

    mlflow.set_experiment(cfg.experiment)
    run_ids: dict[str, str] = {}
    metrics: dict[str, dict[str, float]] = {}
    model_uris: dict[str, str] = {}
    candidates: list[tuple[str, dict[str, Any]]] = [
        ("constant_prior", {}),
        ("logistic", cfg.logistic),
        ("lightgbm", cfg.lightgbm),
    ]
    for model_type, params in candidates:  # baselines first
        extra: dict[str, Any] = {}
        if model_type == "constant_prior":
            pipeline = Pipeline([("model", DummyClassifier(strategy="prior"))]).fit(
                X["train"], y["train"]
            )
        elif model_type == "logistic":
            pipeline = _logistic(cfg).fit(X["train"], y["train"])
        else:
            pipeline, extra["best_iteration"] = _lightgbm(cfg, X, y)
        model = ProbabilityModel(pipeline)
        scores = {
            f"{s}_{k}": v
            for s in ("val", "test")
            for k, v in classification_metrics(y[s], model.predict(None, X[s])).items()
        }
        with mlflow.start_run(run_name=model_type) as run:
            mlflow.set_tags({"model_type": model_type, **lineage})
            mlflow.log_params({**common, **params, **extra})
            mlflow.log_metrics(scores)
            example = X["train"].head(5)
            info = mlflow.pyfunc.log_model(
                name="model",
                python_model=model,
                signature=infer_signature(example, model.predict(None, example)),
                input_example=example,
            )
        run_ids[model_type], metrics[model_type] = run.info.run_id, scores
        model_uris[model_type] = info.model_uri

    version = str(mlflow.register_model(model_uris["lightgbm"], cfg.registered_model).version)
    decision = promote(
        MlflowClient(),
        cfg.registered_model,
        version,
        X["test"],
        y["test"],
        baseline_brier=brier_baseline(metrics),
        mode=promotion_mode,
    )
    return TrainResult(run_ids, metrics, version, decision)
