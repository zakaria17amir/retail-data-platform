"""Delayed ground truth: scored orders that have since been delivered, scored per approval week."""

from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, precision_score, recall_score

THRESHOLD = 0.5
PR_AUC_MARGIN = 0.05
COLUMNS = ["window", "precision", "recall", "pr_auc", "n_labelled", "n_positives"]


def weekly_performance(df: pd.DataFrame) -> pd.DataFrame:
    """Labelled rows (`order_approved_ts_utc, is_late, probability`) → one row per Monday-start
    approval week. Undefined metrics are NaN (PR-AUC for a single-class week, recall without
    positives, precision without predicted positives)."""
    approved = pd.to_datetime(df["order_approved_ts_utc"], utc=True).dt.tz_localize(None)
    week = approved.dt.to_period("W-SUN").dt.start_time
    rows = []
    for start, part in df.groupby(week, sort=True):
        y = part["is_late"].astype(bool).astype(int)
        p = part["probability"].astype(float)
        start = pd.Timestamp(start)
        rows.append(
            {
                "window": f"{start.date()}/{(start + pd.Timedelta(days=7)).date()}",
                "precision": float(precision_score(y, p >= THRESHOLD, zero_division=np.nan)),
                "recall": float(recall_score(y, p >= THRESHOLD, zero_division=np.nan)),
                "pr_auc": float(average_precision_score(y, p)) if y.nunique() == 2 else np.nan,
                "n_labelled": len(part),
                "n_positives": int(y.sum()),
            }
        )
    return pd.DataFrame(rows, columns=COLUMNS)


def supported(weekly: pd.DataFrame, min_labelled: int, min_positives: int) -> pd.Series:
    """Weeks with enough labelled orders and positives for their PR-AUC to gate a breach."""
    return (
        (weekly["n_labelled"] >= min_labelled)
        & (weekly["n_positives"] >= min_positives)
        & weekly["pr_auc"].notna()
    )
