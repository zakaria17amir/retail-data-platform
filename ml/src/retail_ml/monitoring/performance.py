"""Delayed ground truth: scored orders that have since been delivered, scored per approval week."""

from __future__ import annotations

import pandas as pd
from sklearn.metrics import average_precision_score, precision_score, recall_score

THRESHOLD = 0.5
PR_AUC_MARGIN = 0.05


def weekly_performance(df: pd.DataFrame) -> pd.DataFrame:
    """Labelled rows (`order_approved_ts_utc, is_late, probability`) → one row per Monday-start
    approval week: `window, precision, recall, pr_auc, n_labelled`. Weeks with a single class are
    skipped (PR-AUC undefined)."""
    approved = pd.to_datetime(df["order_approved_ts_utc"], utc=True).dt.tz_localize(None)
    week = approved.dt.to_period("W-SUN").dt.start_time
    rows = []
    for start, part in df.groupby(week, sort=True):
        y = part["is_late"].astype(bool).astype(int)
        if y.nunique() < 2:
            continue
        p = part["probability"].astype(float)
        start = pd.Timestamp(start)
        rows.append(
            {
                "window": f"{start.date()}/{(start + pd.Timedelta(days=7)).date()}",
                "precision": float(precision_score(y, p >= THRESHOLD, zero_division=0)),
                "recall": float(recall_score(y, p >= THRESHOLD, zero_division=0)),
                "pr_auc": float(average_precision_score(y, p)),
                "n_labelled": len(part),
            }
        )
    return pd.DataFrame(rows, columns=["window", "precision", "recall", "pr_auc", "n_labelled"])
