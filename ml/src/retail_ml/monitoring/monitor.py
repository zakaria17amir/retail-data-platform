"""`retail-ml monitor late_delivery`: drift + delayed ground truth → rows, exit code, HTML report.

The data is historical, so "now" is the latest approval among the scored orders (not the wall
clock); the current window is the scored orders approved in `(now − 7 days, now]`. The reference is
a seeded sample of the champion's training window, scored by the champion (in-sample, so
`prediction_drift` is biased towards drift and is never a breach condition).
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import mlflow
import numpy as np
import pandas as pd
from botocore.exceptions import BotoCoreError, ClientError  # type: ignore[import-untyped]
from feast import FeatureStore
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

from retail_ml import config
from retail_ml.config import LateDeliveryConfig
from retail_ml.data import historical_seller_features, read_training
from retail_ml.features import SELLER_FEATURES
from retail_ml.late_delivery.promote import CHAMPION
from retail_ml.late_delivery.train import model_inputs, split
from retail_ml.monitoring.drift import PREDICTION, drift_report, upload_report
from retail_ml.monitoring.performance import PR_AUC_MARGIN, supported, weekly_performance

BREACH = 3
WINDOW = pd.Timedelta(days=7)
SEED = 0
COLUMNS = ["run_ts", "metric", "value", "window"]
WEEKLY_METRICS = ("precision", "recall", "pr_auc", "n_labelled", "n_positives")
logger = logging.getLogger(__name__)


def _env(name: str, default: float) -> float:
    return float(os.environ.get(name) or default)


def drift_share_threshold() -> float:
    # each of the 16 features is tested at p < 0.05, so ~0.8 columns drift by chance; > 0.3 needs
    # ≥ 5 (P ≈ 0.1 % under independent nulls, higher with correlated features but still rare)
    return _env("DRIFT_SHARE_THRESHOLD", 0.3)


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


def _append(path: Path, rows: list[dict[str, object]], run_ts: pd.Timestamp) -> None:
    frame = pd.DataFrame(rows).assign(run_ts=run_ts)[COLUMNS]
    if path.exists():
        frame = pd.concat([pd.read_parquet(path), frame], ignore_index=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def _champion(client: MlflowClient, name: str) -> tuple[str, float]:
    """(version, test PR-AUC of its run), with clear errors when the registry can't provide them."""
    try:
        mv = client.get_model_version_by_alias(name, CHAMPION)
    except MlflowException as e:
        raise RuntimeError(f"{name}: no '{CHAMPION}' alias in the registry") from e
    if not mv.run_id:
        raise RuntimeError(f"{name} v{mv.version} ({CHAMPION}) has no source run_id")
    metrics = client.get_run(mv.run_id).data.metrics
    if "test_pr_auc" not in metrics:
        raise RuntimeError(f"{name} v{mv.version} run {mv.run_id} has no test_pr_auc metric")
    return str(mv.version), float(metrics["test_pr_auc"])


def monitor_late_delivery(
    cfg: LateDeliveryConfig, store: FeatureStore, client: MlflowClient
) -> int:
    """0 = healthy (or nothing to monitor), `BREACH` = drift share or weekly PR-AUC breached.
    Rows are written and the breach decided before the (best-effort) report upload."""
    run_ts = pd.Timestamp.now(tz="UTC")
    out = config.gold_dir() / "ml" / "ml_monitoring.parquet"
    scored = _read_scored(config.gold_dir() / "ml" / "pred_late_delivery.parquet")
    if not scored.empty:
        scored = scored.merge(pd.read_parquet(cfg.training_path), on="order_id", how="inner")
    if scored.empty:
        logger.warning("empty current window: no scored orders to monitor; exit 0")
        return 0

    approved = pd.to_datetime(scored["order_approved_ts_utc"], utc=True)
    now = approved.max()
    window = f"{(now - WINDOW).date()}/{now.date()}"
    current = _with_seller(store, scored[approved > now - WINDOW])

    version, champion_pr_auc = _champion(client, cfg.registered_model)
    floor = champion_pr_auc - PR_AUC_MARGIN
    model = mlflow.pyfunc.load_model(f"models:/{cfg.registered_model}/{version}")
    train = split(read_training(cfg.training_path), cfg)["train"]
    n_reference = int(_env("REFERENCE_SAMPLE_ROWS", 10_000))
    if len(train) > n_reference:
        train = train.sample(n=n_reference, random_state=SEED)
    reference = _with_seller(store, train)
    reference[PREDICTION] = np.asarray(model.predict(model_inputs(reference)), dtype=float)
    drift, html = drift_report(reference, current)

    weekly = weekly_performance(scored[scored["is_late"].notna()])
    rows: list[dict[str, object]] = [
        *({"metric": k, "value": v, "window": window} for k, v in drift.items()),
        {"metric": "n_current", "value": float(len(current)), "window": window},
        *(
            {"metric": m, "value": float(r[m]), "window": r["window"]}
            for r in weekly.to_dict("records")
            for m in WEEKLY_METRICS
            if pd.notna(r[m])
        ),
    ]
    _append(out, rows, run_ts)

    breaches = []
    threshold = drift_share_threshold()
    min_current = int(_env("MIN_CURRENT_ROWS", 30))
    if len(current) < min_current:
        logger.warning(
            "current window has %d rows < MIN_CURRENT_ROWS %d: drift recorded, not gated",
            len(current),
            min_current,
        )
    elif drift["drift_share"] > threshold:
        breaches.append(f"drift_share {drift['drift_share']:.3f} > {threshold}")

    ok = supported(weekly, int(_env("MIN_LABELLED", 100)), int(_env("MIN_POSITIVES", 10)))
    if (~ok).any():
        logger.warning(
            "weeks below support (MIN_LABELLED/MIN_POSITIVES or single class), recorded but "
            "not gated: %s",
            ", ".join(weekly.loc[~ok, "window"]),
        )
    if ok.any():
        latest = weekly[ok].iloc[-1]
        if latest["pr_auc"] < floor:
            breaches.append(f"pr_auc {latest['pr_auc']:.4f} ({latest['window']}) < {floor:.4f}")
    else:
        logger.warning("no supported delayed ground-truth week yet; PR-AUC not gated")
    for breach in breaches:
        logger.warning("breach: %s", breach)

    uri = f"{report_root()}/{cfg.registered_model}/{now.date()}.html"
    try:
        upload_report(html, uri)
        uploaded = 1.0
    except (BotoCoreError, ClientError, OSError):
        logger.exception("report upload to %s failed; rows and exit code unaffected", uri)
        uploaded = 0.0
    _append(out, [{"metric": "report_uploaded", "value": uploaded, "window": window}], run_ts)

    summary = {"window": window, **drift, "pr_auc_floor": floor, "breaches": breaches}
    print(json.dumps({**summary, "report": uri if uploaded else None}))
    return BREACH if breaches else 0
