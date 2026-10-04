"""Champion/challenger promotion on the same test window (rules: plan Global Constraints)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import mlflow
import numpy as np
import pandas as pd
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

from retail_ml.late_delivery.evaluate import (
    PRIMARY_METRIC,
    classification_metrics,
    model_test_failures,
)

CHAMPION, CHALLENGER = "champion", "challenger"


@dataclass(frozen=True)
class Decision:
    promoted: bool
    reason: str
    challenger_pr_auc: float
    champion_pr_auc: float | None


def _predict(uri: str, X: pd.DataFrame) -> Any:
    return np.asarray(mlflow.pyfunc.load_model(uri).predict(X), dtype=float)


def _champion_version(client: MlflowClient, name: str) -> str | None:
    try:
        return str(client.get_model_version_by_alias(name, CHAMPION).version)
    except MlflowException:
        return None


def approve(client: MlflowClient, name: str, version: str) -> None:
    client.set_registered_model_alias(name, CHAMPION, version)


def promote(
    client: MlflowClient,
    name: str,
    version: str,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    baseline_brier: float,
    mode: str = "auto",
) -> Decision:
    """Set `challenger` on `version`; move `champion` to it in auto mode when it passes the model
    tests and beats the current champion's PR-AUC re-scored on the same test rows."""
    client.set_registered_model_alias(name, CHALLENGER, version)
    p = _predict(f"models:/{name}/{version}", X_test)
    failures = model_test_failures(y_test, p, baseline_brier)
    valid = bool(np.isfinite(p).all() and ((p >= 0) & (p <= 1)).all())
    score = _score(y_test, p) if valid else float("nan")
    champion = _champion_version(client, name)
    champion_score = None
    if champion is not None:
        champion_score = _score(y_test, _predict(f"models:/{name}/{champion}", X_test))

    def decide(promoted: bool, reason: str) -> Decision:
        return Decision(promoted, reason, score, champion_score)

    if failures:
        return decide(False, "model tests failed: " + "; ".join(failures))
    if champion_score is not None and not score > champion_score:
        return decide(
            False, f"{PRIMARY_METRIC} {score:.4f} <= champion v{champion} {champion_score:.4f}"
        )
    if mode == "manual":
        print(f"approve with: retail-ml promote {name} --version {version}")
        return decide(False, "manual mode: awaiting approval")
    approve(client, name, version)
    return decide(True, "no champion" if champion is None else f"beats champion v{champion}")


def _score(y: pd.Series, p: Any) -> float:
    return classification_metrics(y, p)[PRIMARY_METRIC]
