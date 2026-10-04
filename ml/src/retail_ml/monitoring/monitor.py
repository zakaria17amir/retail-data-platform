"""`retail-ml monitor late_delivery`: drift + delayed ground truth → HTML report, rows, exit code.

The data is historical, so "now" is the latest approval among the scored orders (not the wall
clock); the current window is the scored orders approved in `(now − 7 days, now]`.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import mlflow
import numpy as np
import pandas as pd
from feast import FeatureStore
from mlflow import MlflowClient

from retail_ml import config
from retail_ml.config import LateDeliveryConfig
from retail_ml.data import historical_seller_features, read_training
from retail_ml.features import SELLER_FEATURES
from retail_ml.late_delivery.promote import CHAMPION
from retail_ml.late_delivery.train import model_inputs, split
from retail_ml.monitoring.drift import PREDICTION, drift_report, upload_report
from retail_ml.monitoring.performance import PR_AUC_MARGIN, weekly_performance

BREACH = 3
WINDOW = pd.Timedelta(days=7)
logger = logging.getLogger(__name__)


def drift_share_threshold() -> float:
    return float(os.environ.get("DRIFT_SHARE_THRESHOLD") or 0.3)


def report_root() -> str:
    return (os.environ.get("MONITORING_REPORT_ROOT") or "s3://lakehouse/monitoring").rstrip("/")


def _read_scored(path: Path) -> pd.DataFrame:
    """Latest score per order (the file may hold several scoring runs)."""
    if not path.exists():
        return pd.DataFrame(columns=["order_id", PREDICTION])
    df = pd.read_parquet(path).sort_values("scored_ts", kind="stable")
    return df.drop_duplicates("order_id", keep="last")[["order_id", PREDICTION]]


def _with_seller(store: FeatureStore, df: pd.DataFrame) -> pd.DataFrame:
    df = df.reset_index(drop=True)
    seller = historical_seller_features(store, df)
    return pd.concat([df, seller[SELLER_FEATURES]], axis=1)


def _append(path: Path, rows: pd.DataFrame) -> None:
    if path.exists():
        rows = pd.concat([pd.read_parquet(path), rows], ignore_index=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    rows.to_parquet(path, index=False)


def monitor_late_delivery(
    cfg: LateDeliveryConfig, store: FeatureStore, client: MlflowClient
) -> int:
    """0 = healthy (or nothing to monitor), `BREACH` = drift share or weekly PR-AUC breached."""
    run_ts = pd.Timestamp.now(tz="UTC")
    ml_dir = config.gold_dir() / "ml"
    scored = _read_scored(ml_dir / "pred_late_delivery.parquet")
    if not scored.empty:
        scored = scored.merge(pd.read_parquet(cfg.training_path), on="order_id", how="inner")
    if scored.empty:
        logger.warning("empty current window: no scored orders to monitor; exit 0")
        return 0

    approved = pd.to_datetime(scored["order_approved_ts_utc"], utc=True)
    now = approved.max()
    window = f"{(now - WINDOW).date()}/{now.date()}"
    current = _with_seller(store, scored[approved > now - WINDOW])

    champion = client.get_model_version_by_alias(cfg.registered_model, CHAMPION)
    assert champion.run_id is not None
    floor = client.get_run(champion.run_id).data.metrics["test_pr_auc"] - PR_AUC_MARGIN
    model = mlflow.pyfunc.load_model(f"models:/{cfg.registered_model}@{CHAMPION}")
    reference = _with_seller(store, split(read_training(cfg.training_path), cfg)["train"])
    reference[PREDICTION] = np.asarray(model.predict(model_inputs(reference)), dtype=float)

    drift, html = drift_report(reference, current)
    upload_report(html, f"{report_root()}/{cfg.registered_model}/{now.date()}.html")

    weekly = weekly_performance(scored[scored["is_late"].notna()])
    rows = [
        *({"metric": k, "value": v, "window": window} for k, v in drift.items()),
        {"metric": "n_current", "value": float(len(current)), "window": window},
        *(
            {"metric": m, "value": float(r[m]), "window": r["window"]}
            for r in weekly.to_dict("records")
            for m in ("precision", "recall", "pr_auc", "n_labelled")
        ),
    ]
    _append(
        ml_dir / "ml_monitoring.parquet",
        pd.DataFrame(rows).assign(run_ts=run_ts)[["run_ts", "metric", "value", "window"]],
    )

    threshold = drift_share_threshold()
    breaches = []
    if drift["drift_share"] > threshold:
        breaches.append(f"drift_share {drift['drift_share']:.3f} > {threshold}")
    if weekly.empty:
        logger.warning("no delayed ground truth yet: no scored order has been delivered")
    elif (latest := float(weekly["pr_auc"].iloc[-1])) < floor:
        breaches.append(f"pr_auc {latest:.4f} ({weekly['window'].iloc[-1]}) < floor {floor:.4f}")
    for breach in breaches:
        logger.warning("breach: %s", breach)
    print(json.dumps({"window": window, **drift, "pr_auc_floor": floor, "breaches": breaches}))
    return BREACH if breaches else 0
