import logging
from pathlib import Path
from typing import Any

import mlflow
import numpy as np
import pandas as pd
import pytest
from conftest import local_store, synthetic_gold
from feast import FeatureStore
from mlflow import MlflowClient
from mlflow.pyfunc.model import PythonModel

from retail_ml import cli
from retail_ml.config import load_late_delivery_config
from retail_ml.monitoring import drift
from retail_ml.monitoring.monitor import BREACH, monitor_late_delivery
from retail_ml.monitoring.performance import weekly_performance

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


def write_scored(gold: Path, days: int = 28, good_model: bool = True) -> pd.Timestamp:
    """Pred file over the last `days` of orders; P(late) from the labels (good or inverted)."""
    df = pd.read_parquet(training_path(gold))
    approved = pd.to_datetime(df["order_approved_ts_utc"], utc=True)
    df = df[approved > approved.max() - pd.Timedelta(days=days)]
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


def test_unshifted_data_and_good_model_exit_0_with_report_and_rows(env: dict[str, Any]) -> None:
    now = write_scored(env["gold"])
    assert run(env) == 0
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


def test_delayed_ground_truth_below_champion_floor_exit_3(env: dict[str, Any]) -> None:
    write_scored(env["gold"], good_model=False)
    assert run(env) == 3
    rows = monitoring_rows(env["gold"])
    assert rows.loc[rows["metric"] == "drift_share", "value"].item() <= 0.3
    assert rows.loc[rows["metric"] == "pr_auc", "value"].iloc[-1] < 0.9 - 0.05


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


def test_weekly_performance_groups_by_approval_week_and_skips_single_class() -> None:
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
    assert out["window"].tolist() == ["2018-08-06/2018-08-13"]
    row = out.iloc[0]
    assert row["precision"] == pytest.approx(0.5)
    assert row["recall"] == pytest.approx(1.0)
    assert row["pr_auc"] == pytest.approx(1.0)
    assert row["n_labelled"] == 3


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
