"""Champion/challenger promotion on the same test window (rules: plan Global Constraints)."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import mlflow
import numpy as np
import pandas as pd
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

from retail_ml.config import check_promotion_mode
from retail_ml.late_delivery.evaluate import (
    PRIMARY_METRIC,
    classification_metrics,
    model_test_failures,
)

CHAMPION, CHALLENGER = "champion", "challenger"
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Decision:
    promoted: bool
    reason: str
    challenger_pr_auc: float
    champion_pr_auc: float | None


def _predict(uri: str, X: pd.DataFrame) -> Any:
    return np.asarray(mlflow.pyfunc.load_model(uri).predict(X), dtype=float)


def champion_version(client: MlflowClient, name: str) -> str | None:
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
    mode = check_promotion_mode(mode)
    client.set_registered_model_alias(name, CHALLENGER, version)
    p = _predict(f"models:/{name}/{version}", X_test)
    failures = model_test_failures(y_test, p, baseline_brier)
    if failures:
        valid = bool(np.isfinite(p).all() and ((p >= 0) & (p <= 1)).all())
        score = _score(y_test, p) if valid else float("nan")
        return Decision(False, "model tests failed: " + "; ".join(failures), score, None)

    score = _score(y_test, p)
    champion = champion_version(client, name)
    champion_score = None
    reason = "no champion"
    if champion is not None:
        try:
            champion_score = _score(y_test, _predict(f"models:/{name}/{champion}", X_test))
            reason = f"beats champion v{champion}"
        except Exception:
            logger.exception("champion v%s of %s could not be scored", champion, name)
            reason = "champion could not be scored"

    if champion_score is not None and not score > champion_score:
        reason = f"{PRIMARY_METRIC} {score:.4f} <= champion v{champion} {champion_score:.4f}"
        return Decision(False, reason, score, champion_score)
    if mode == "manual":
        print(f"approve with: retail-ml promote {name} --version {version}")
        return Decision(False, f"manual mode: awaiting approval ({reason})", score, champion_score)
    approve(client, name, version)
    return Decision(True, reason, score, champion_score)


def _score(y: pd.Series, p: Any) -> float:
    return classification_metrics(y, p)[PRIMARY_METRIC]
