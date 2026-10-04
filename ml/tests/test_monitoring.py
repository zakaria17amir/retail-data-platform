import logging
from pathlib import Path
from typing import Any

import mlflow
import numpy as np
import pandas as pd
import pytest
from botocore.exceptions import ClientError
from conftest import local_store, synthetic_gold
from feast import FeatureStore
from mlflow import MlflowClient
from mlflow.pyfunc.model import PythonModel

from retail_ml import cli
from retail_ml.config import load_late_delivery_config
from retail_ml.features import CATEGORICAL
from retail_ml.monitoring import drift, monitor
from retail_ml.monitoring.monitor import BREACH, monitor_late_delivery
from retail_ml.monitoring.performance import supported, weekly_performance

COLUMNS = ["run_ts", "metric", "value", "window"]


class StubModel(PythonModel):
    def predict(self, context: Any, model_input: pd.DataFrame, params: Any = None) -> Any:
        return model_input["seller_late_rate_90d"].fillna(0.2).clip(0, 1).to_numpy()


def register_champion(test_pr_auc: float) -> None:
    with mlflow.start_run():
        mlflow.log_metric("test_pr_auc", test_pr_auc)
        info = mlflow.pyfunc.log_model(name="model", python_model=StubModel())
    version = mlflow.register_model(info.model_uri, "late_delivery").version
    MlflowClient().set_registered_model_alias("late_delivery", "champion", str(version))


@pytest.fixture
def env(
    gold_dir: Path, mlflow_uri: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> dict[str, Any]:
    synthetic_gold(gold_dir)
    monkeypatch.setenv("MONITORING_REPORT_ROOT", str(tmp_path / "reports"))
    register_champion(test_pr_auc=0.9)
    return {"gold": gold_dir, "store": local_store(tmp_path), "reports": tmp_path / "reports"}


def training_path(gold: Path) -> Path:
    return gold / "ml" / "late_delivery_training.parquet"


def write_scored(
    gold: Path, days: int = 28, good_model: bool = True, current_rows: int | None = None
) -> pd.Timestamp:
    """Pred file over the last `days` of orders; P(late) from the labels (good or inverted).
    `current_rows` keeps only that many orders of the last 7 days (the latest one always)."""
    df = pd.read_parquet(training_path(gold))
    approved = pd.to_datetime(df["order_approved_ts_utc"], utc=True)
    df = df[approved > approved.max() - pd.Timedelta(days=days)]
    if current_rows is not None:
        approved = pd.to_datetime(df["order_approved_ts_utc"], utc=True)
        in_current = approved > approved.max() - pd.Timedelta(days=7)
        newest = approved[in_current].sort_values(ascending=False).index[:current_rows]
        df = df[~in_current | df.index.isin(newest)]
    late = df["is_late"].fillna(False).astype(bool).to_numpy()
    p = np.where(late == good_model, 0.9, 0.1)
    pd.DataFrame(
        {
            "order_id": df["order_id"].to_numpy(),
            "probability": p,
            "model_version": "1",
            "scored_ts": pd.Timestamp("2026-01-01", tz="UTC"),
        }
    ).to_parquet(gold / "ml" / "pred_late_delivery.parquet", index=False)
    return pd.Timestamp(approved.max())


def shift_last_week(gold: Path) -> None:
    df = pd.read_parquet(training_path(gold))
    approved = pd.to_datetime(df["order_approved_ts_utc"], utc=True)
    last = approved > approved.max() - pd.Timedelta(days=7)
    df.loc[last, "total_freight"] = df.loc[last, "total_freight"] * 10
    df.loc[last, "customer_lat"] = df.loc[last, "customer_lat"] + 15
    df.loc[last, "order_estimated_delivery_ts_utc"] = approved[last] + pd.Timedelta(days=90)
    df.loc[last, "payment_type"] = "voucher"
    df.loc[last, "product_category"] = "toys"
    df.loc[last, "customer_state"] = "BA"
    df.loc[last, "payment_installments"] = 24
    df.loc[last, "n_items"] = 9
    df.to_parquet(training_path(gold), index=False)


def monitoring_rows(gold: Path) -> pd.DataFrame:
    return pd.read_parquet(gold / "ml" / "ml_monitoring.parquet")


def run(env: dict[str, Any]) -> int:
    store: FeatureStore = env["store"]
    return monitor_late_delivery(load_late_delivery_config(), store, MlflowClient())


def metric(rows: pd.DataFrame, name: str) -> pd.Series:
    return rows.loc[rows["metric"] == name, "value"]


def test_unshifted_data_and_good_model_exit_0_with_report_and_rows(
    env: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    joined: list[int] = []
    original = monitor.historical_seller_features

    def spy(store: FeatureStore, orders: pd.DataFrame) -> pd.DataFrame:
        joined.append(len(orders))
        return original(store, orders)

    monkeypatch.setattr(monitor, "historical_seller_features", spy)
    monkeypatch.setenv("REFERENCE_SAMPLE_ROWS", "1000")
    now = write_scored(env["gold"])
    assert run(env) == 0
    assert 1000 in joined  # reference sampled before the Feast join
    assert metric(monitoring_rows(env["gold"]), "report_uploaded").item() == 1.0
    assert (env["reports"] / "late_delivery" / f"{now.date()}.html").stat().st_size > 0
    rows = monitoring_rows(env["gold"])
    assert list(rows.columns) == COLUMNS
    metrics = dict(zip(rows["metric"], rows["value"], strict=False))
    assert metrics["drift_share"] <= 0.3
    assert {"prediction_drift", "n_current", "precision", "recall", "pr_auc"} <= set(metrics)
    drift_window = rows.loc[rows["metric"] == "drift_share", "window"].item()
    assert drift_window == f"{(now - pd.Timedelta(days=7)).date()}/{now.date()}"
    assert rows.loc[rows["metric"] == "pr_auc", "value"].min() == pytest.approx(1.0)


def test_shifted_features_exit_3_via_cli(
    env: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    write_scored(env["gold"])
    shift_last_week(env["gold"])
    monkeypatch.setattr(cli, "feature_store", lambda: env["store"])
    assert cli.main(["monitor", "late_delivery"]) == BREACH == 3
    rows = monitoring_rows(env["gold"])
    assert rows.loc[rows["metric"] == "drift_share", "value"].item() > 0.3


def test_delayed_ground_truth_below_champion_floor_exit_3(
    env: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MIN_LABELLED", "20")
    monkeypatch.setenv("MIN_POSITIVES", "3")
    write_scored(env["gold"], good_model=False)
    assert run(env) == 3
    rows = monitoring_rows(env["gold"])
    assert rows.loc[rows["metric"] == "drift_share", "value"].item() <= 0.3
    assert rows.loc[rows["metric"] == "pr_auc", "value"].iloc[-1] < 0.9 - 0.05


def test_tiny_noisy_weeks_are_recorded_but_do_not_breach(
    env: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    write_scored(env["gold"], good_model=False)  # ~20-40 labelled per week < MIN_LABELLED 100
    with caplog.at_level(logging.WARNING):
        assert run(env) == 0
    assert "below support" in caplog.text
    rows = monitoring_rows(env["gold"])
    assert (metric(rows, "pr_auc") < 0.85).all()
    assert len(metric(rows, "n_positives")) == len(metric(rows, "n_labelled")) >= 4


def test_small_current_window_warns_and_does_not_breach_on_drift(
    env: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    write_scored(env["gold"], current_rows=10)
    shift_last_week(env["gold"])
    with caplog.at_level(logging.WARNING):
        assert run(env) == 0
    assert "MIN_CURRENT_ROWS" in caplog.text
    rows = monitoring_rows(env["gold"])
    assert metric(rows, "n_current").item() == 10


def test_failed_upload_still_writes_rows_and_exits_3(
    env: dict[str, Any], monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    class FailingS3:
        def put_object(self, **kwargs: Any) -> None:
            raise ClientError({"Error": {"Code": "NoSuchBucket", "Message": "x"}}, "PutObject")

    monkeypatch.setenv("MONITORING_REPORT_ROOT", "s3://lakehouse/monitoring")
    monkeypatch.setattr(drift.boto3, "client", lambda *a, **k: FailingS3())
    write_scored(env["gold"])
    shift_last_week(env["gold"])
    with caplog.at_level(logging.WARNING):
        assert run(env) == 3
    assert "upload" in caplog.text
    rows = monitoring_rows(env["gold"])
    assert metric(rows, "drift_share").item() > 0.3
    assert metric(rows, "report_uploaded").item() == 0.0


def test_empty_current_window_exit_0_with_warning(
    env: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    pd.DataFrame(
        {
            "order_id": pd.Series([], dtype=str),
            "probability": pd.Series([], dtype=float),
            "model_version": pd.Series([], dtype=str),
            "scored_ts": pd.Series([], dtype="datetime64[ns, UTC]"),
        }
    ).to_parquet(env["gold"] / "ml" / "pred_late_delivery.parquet", index=False)
    with caplog.at_level(logging.WARNING):
        assert run(env) == 0
    assert "empty current window" in caplog.text
    assert not env["reports"].exists()


def test_missing_pred_file_exit_0_with_warning(
    env: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.WARNING):
        assert run(env) == 0
    assert "empty current window" in caplog.text


def test_rows_are_appended_across_runs(env: dict[str, Any]) -> None:
    write_scored(env["gold"])
    run(env)
    first = len(monitoring_rows(env["gold"]))
    run(env)
    rows = monitoring_rows(env["gold"])
    assert len(rows) == 2 * first
    assert rows["run_ts"].nunique() == 2


def test_weekly_performance_groups_by_approval_week_single_class_pr_auc_undefined() -> None:
    df = pd.DataFrame(
        {
            # 2018-08-06 and 2018-08-13 are Mondays
            "order_approved_ts_utc": pd.to_datetime(
                ["2018-08-06", "2018-08-08", "2018-08-12 23:00", "2018-08-13", "2018-08-14"],
                utc=True,
                format="ISO8601",
            ),
            "is_late": [True, False, False, False, False],
            "probability": [0.9, 0.6, 0.1, 0.2, 0.7],
        }
    )
    out = weekly_performance(df)
    assert out["window"].tolist() == ["2018-08-06/2018-08-13", "2018-08-13/2018-08-20"]
    first, single = out.iloc[0], out.iloc[1]
    assert first["precision"] == pytest.approx(0.5)
    assert first["recall"] == pytest.approx(1.0)
    assert first["pr_auc"] == pytest.approx(1.0)
    assert (first["n_labelled"], first["n_positives"]) == (3, 1)
    assert (single["n_labelled"], single["n_positives"]) == (2, 0)
    assert np.isnan(single["pr_auc"]) and np.isnan(single["recall"])


def test_supported_weeks_need_min_labelled_and_positives() -> None:
    weekly = pd.DataFrame(
        {
            "window": ["w1", "w2", "w3", "w4"],
            "pr_auc": [0.6, 0.7, 0.1, np.nan],
            "n_labelled": [150, 120, 30, 200],
            "n_positives": [20, 12, 9, 0],
        }
    )
    assert supported(weekly, min_labelled=100, min_positives=10).tolist() == [
        True,
        True,
        False,
        False,
    ]


def test_rare_categories_collapse_to_other_by_reference_share() -> None:
    ref = pd.DataFrame({"payment_type": ["a"] * 60 + ["b"] * 37 + ["c"] * 3})
    cur = pd.DataFrame({"payment_type": ["a", "c", "z"]})
    for c in CATEGORICAL[1:]:
        ref[c], cur[c] = "x", "x"
    r, c = drift.collapse_rare(ref, cur)
    assert sorted(set(r["payment_type"])) == ["__other__", "a", "b"]
    assert c["payment_type"].tolist() == ["a", "__other__", "__other__"]
    assert ref["payment_type"].iloc[-1] == "c"  # inputs untouched


def test_upload_report_to_s3_uses_minio_endpoint_and_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, Any] = {}

    class FakeS3:
        def put_object(self, **kwargs: Any) -> None:
            calls["put"] = kwargs

    def fake_client(service: str, **kwargs: Any) -> FakeS3:
        calls["client"] = (service, kwargs)
        return FakeS3()

    monkeypatch.setenv("MINIO_ENDPOINT", "http://minio:9000")
    monkeypatch.setenv("MINIO_ROOT_USER", "u")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "p")
    monkeypatch.setattr(drift.boto3, "client", fake_client)
    drift.upload_report("<html/>", "s3://lakehouse/monitoring/late_delivery/2018-09-01.html")
    assert calls["client"] == (
        "s3",
        {
            "endpoint_url": "http://minio:9000",
            "aws_access_key_id": "u",
            "aws_secret_access_key": "p",
        },
    )
    put = calls["put"]
    assert (put["Bucket"], put["Key"]) == ("lakehouse", "monitoring/late_delivery/2018-09-01.html")
    assert put["Body"] == b"<html/>"
    assert put["ContentType"] == "text/html"


def test_upload_report_to_file_uri(tmp_path: Path) -> None:
    target = tmp_path / "r" / "late_delivery" / "x.html"
    drift.upload_report("<html/>", target.as_uri())
    assert target.read_text(encoding="utf-8") == "<html/>"
