"""Classification metrics and the model tests gating promotion."""

from __future__ import annotations

from typing import Any

import numpy as np
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    precision_recall_curve,
    roc_auc_score,
)

PRIMARY_METRIC = "pr_auc"


def recall_at_precision(y: Any, p: Any, min_precision: float = 0.5) -> float:
    precision, recall, _ = precision_recall_curve(y, p)
    ok = recall[precision >= min_precision]
    return float(ok.max()) if ok.size else 0.0


def classification_metrics(y: Any, p: Any) -> dict[str, float]:
    y, p = np.asarray(y, dtype=int), np.asarray(p, dtype=float)
    return {
        "pr_auc": float(average_precision_score(y, p)),
        "roc_auc": float(roc_auc_score(y, p)),
        "brier": float(brier_score_loss(y, p)),
        "recall_at_p50": recall_at_precision(y, p),
    }


def brier_baseline(metrics: dict[str, dict[str, float]]) -> float:
    """Calibration bar: test Brier of the better of the constant prior and logistic."""
    return min(metrics["constant_prior"]["test_brier"], metrics["logistic"]["test_brier"])


def model_test_failures(y: Any, p: Any, baseline_brier: float) -> list[str]:
    """Empty list = passes: no NaN, range [0, 1], Brier <= min(prior, logistic) baseline."""
    p = np.asarray(p, dtype=float)
    if np.isnan(p).any():
        return ["nan predictions"]
    failures = []
    if ((p < 0) | (p > 1)).any():
        failures.append("predictions out of range [0, 1]")
    brier = float(np.mean((p - np.asarray(y, dtype=float)) ** 2))
    if brier > baseline_brier:
        failures.append(f"brier {brier:.4f} > baseline {baseline_brier:.4f} (min prior/logistic)")
    return failures
