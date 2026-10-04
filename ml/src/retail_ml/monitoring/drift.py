"""Evidently feature + prediction drift (reference vs current) and the HTML report upload."""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import url2pathname

import boto3  # type: ignore[import-untyped]
import pandas as pd
from evidently import DataDefinition, Dataset, Report  # type: ignore[import-untyped]
from evidently.metrics import DriftedColumnsCount, ValueDrift  # type: ignore[import-untyped]
from evidently.presets import DataDriftPreset  # type: ignore[import-untyped]

from retail_ml.features import CATEGORICAL, NUMERIC, SELLER_FEATURES, order_features

PREDICTION = "probability"
# approve_month always drifts for a 7-day window against a year-long reference
DRIFT_NUMERIC = [c for c in NUMERIC if c != "approve_month"] + SELLER_FEATURES
DRIFT_FEATURES = DRIFT_NUMERIC + CATEGORICAL
# p-value tests (drift at p < 0.05) account for the small 7-day window; Evidently's default for a
# reference > 1000 rows (Wasserstein / Jensen-Shannon distance ≥ 0.1) is biased upward at n ≈ 50
METHODS = {"num_method": "ks", "cat_method": "chisquare"}


def drift_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Model features (the same `order_features` the model applies) + seller features + P(late)."""
    out = order_features(df)
    for col in CATEGORICAL:
        out[col] = out[col].astype(str)
    for col in SELLER_FEATURES:
        out[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
    out[PREDICTION] = pd.to_numeric(df[PREDICTION]).astype("float64").to_numpy()
    return out[[*DRIFT_FEATURES, PREDICTION]]


def drift_report(reference: pd.DataFrame, current: pd.DataFrame) -> tuple[dict[str, float], str]:
    """`drift_share` over the features, `prediction_drift` (1.0 = P(late) drifted) and the HTML."""
    definition = DataDefinition(
        numerical_columns=[*DRIFT_NUMERIC, PREDICTION], categorical_columns=CATEGORICAL
    )
    features = DriftedColumnsCount(columns=DRIFT_FEATURES, **METHODS)
    prediction = DriftedColumnsCount(columns=[PREDICTION], **METHODS)
    report = Report(
        [
            DataDriftPreset(columns=DRIFT_FEATURES, **METHODS),
            ValueDrift(column=PREDICTION, method=METHODS["num_method"]),
            features,
            prediction,
        ]
    )
    snapshot = report.run(
        Dataset.from_pandas(drift_frame(current), data_definition=definition),
        Dataset.from_pandas(drift_frame(reference), data_definition=definition),
    )

    def share(metric: DriftedColumnsCount) -> float:
        return float(snapshot.metric_results[metric.metric_id].to_dict()["value"]["share"])

    metrics = {"drift_share": share(features), "prediction_drift": share(prediction)}
    return metrics, str(snapshot.get_html_str(as_iframe=False))


def upload_report(html: str, uri: str) -> None:
    """`s3://bucket/key` via boto3 (MinIO endpoint/credentials, else the AWS_* chain);
    `file://` URIs and plain paths are written locally."""
    url = urlsplit(uri)
    if url.scheme == "s3":
        client = boto3.client(
            "s3",
            endpoint_url=os.environ.get("MINIO_ENDPOINT") or os.environ.get("AWS_ENDPOINT_URL"),
            aws_access_key_id=os.environ.get("MINIO_ROOT_USER") or None,
            aws_secret_access_key=os.environ.get("MINIO_ROOT_PASSWORD") or None,
        )
        client.put_object(
            Bucket=url.netloc,
            Key=url.path.lstrip("/"),
            Body=html.encode("utf-8"),
            ContentType="text/html",
        )
        return
    path = Path(url2pathname(url.path)) if url.scheme == "file" else Path(uri)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html, encoding="utf-8")
