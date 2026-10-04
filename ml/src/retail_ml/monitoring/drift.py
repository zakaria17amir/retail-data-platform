"""Evidently feature + prediction drift (reference vs current) and the HTML report upload."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from urllib.request import url2pathname

import boto3  # type: ignore[import-untyped]
import pandas as pd
from evidently import DataDefinition, Dataset, Report  # type: ignore[import-untyped]
from evidently.metrics import DriftedColumnsCount, ValueDrift  # type: ignore[import-untyped]
from evidently.presets import DataDriftPreset  # type: ignore[import-untyped]

from retail_ml.features import CATEGORICAL, NUMERIC, SELLER_FEATURES, order_features

PREDICTION = "probability"
# approve_month always drifts for a 28-day window against a year-long reference
DRIFT_NUMERIC = [c for c in NUMERIC if c != "approve_month"] + SELLER_FEATURES
DRIFT_FEATURES = DRIFT_NUMERIC + CATEGORICAL
# Evidently's distance defaults (Wasserstein normed / Jensen-Shannon ≥ 0.1) are biased upward at
# n ≈ 50, while p-value tests (p < 0.05) flag negligible shifts once the current window is large
LARGE_WINDOW = 1000
P_VALUE_TESTS = {
    "num_method": "ks",
    "cat_method": "chisquare",
    "num_threshold": 0.05,
    "cat_threshold": 0.05,
}
DISTANCES = {
    "num_method": "wasserstein",
    "cat_method": "jensenshannon",
    "num_threshold": 0.1,
    "cat_threshold": 0.1,
}
# chi-square is unreliable with sparse cells: levels under 5 % of the reference (and levels unseen
# in it) are pooled, so a new dominant level still shows up as a shift in OTHER's share
OTHER = "__other__"
MIN_CATEGORY_SHARE = 0.05


def drift_methods(n_current: int) -> dict[str, Any]:
    return dict(P_VALUE_TESTS if n_current < LARGE_WINDOW else DISTANCES)


def drift_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Model features (the same `order_features` the model applies) + seller features + P(late)."""
    out = order_features(df)
    for col in CATEGORICAL:
        out[col] = out[col].astype(str)
    for col in SELLER_FEATURES:
        out[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
    out[PREDICTION] = pd.to_numeric(df[PREDICTION]).astype("float64").to_numpy()
    return out[[*DRIFT_FEATURES, PREDICTION]]


def collapse_rare(
    reference: pd.DataFrame, current: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Categoricals: levels below `MIN_CATEGORY_SHARE` of the reference → `OTHER` (copies)."""
    reference, current = reference.copy(), current.copy()
    for col in CATEGORICAL:
        share = reference[col].value_counts(normalize=True)
        keep = share.index[share >= MIN_CATEGORY_SHARE]
        reference[col] = reference[col].where(reference[col].isin(keep), OTHER)
        current[col] = current[col].where(current[col].isin(keep), OTHER)
    return reference, current


def drift_report(reference: pd.DataFrame, current: pd.DataFrame) -> tuple[dict[str, float], str]:
    """`drift_share` over the features, `prediction_drift` (1.0 = P(late) drifted) and the HTML.

    `prediction_drift` compares served scores with the champion's *in-sample* scores on its own
    training window, so it is biased towards drift; it is reported, never a breach condition."""
    ref, cur = collapse_rare(drift_frame(reference), drift_frame(current))
    definition = DataDefinition(
        numerical_columns=[*DRIFT_NUMERIC, PREDICTION], categorical_columns=CATEGORICAL
    )
    methods = drift_methods(len(cur))
    features = DriftedColumnsCount(columns=DRIFT_FEATURES, **methods)
    prediction = DriftedColumnsCount(columns=[PREDICTION], **methods)
    report = Report(
        [
            DataDriftPreset(columns=DRIFT_FEATURES, **methods),
            ValueDrift(
                column=PREDICTION, method=methods["num_method"], threshold=methods["num_threshold"]
            ),
            features,
            prediction,
        ]
    )
    snapshot = report.run(
        Dataset.from_pandas(cur, data_definition=definition),
        Dataset.from_pandas(ref, data_definition=definition),
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
