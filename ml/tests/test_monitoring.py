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


class OracleModel(PythonModel):
    """P(late) from the known labels by approval time: 0.9/0.1 (good) or inverted (degraded)."""

    def __init__(self, labels: dict[pd.Timestamp, float]) -> None:
        self.labels = labels

    def predict(self, context: Any, model_input: pd.DataFrame, params: Any = None) -> Any:
        return model_input["order_approved_ts_utc"].map(self.labels).fillna(0.5).to_numpy()


def register_champion(test_pr_auc: float, model: PythonModel | None = None) -> None:
    with mlflow.start_run():
        mlflow.log_metric("test_pr_auc", test_pr_auc)
        info = mlflow.pyfunc.log_model(name="model", python_model=model or StubModel())
    version = mlflow.register_model(info.model_uri, "late_delivery").version
    MlflowClient().set_registered_model_alias("late_delivery", "champion", str(version))


def register_oracle(gold: Path, good_model: bool) -> None:
    df = pd.read_parquet(training_path(gold))
    df = df[df["is_late"].notna()]
    approved = pd.to_datetime(df["order_approved_ts_utc"], utc=True).dt.tz_localize(None)
    p = np.where(df["is_late"].astype(bool).to_numpy() == good_model, 0.9, 0.1)
    register_champion(0.9, OracleModel(dict(zip(approved, p, strict=True))))


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


def shift_tail(gold: Path, days: int = 28) -> None:
    df = pd.read_parquet(training_path(gold))
    approved = pd.to_datetime(df["order_approved_ts_utc"], utc=True)
    last = approved > approved.max() - pd.Timedelta(days=days)
    df.loc[last, "total_freight"] = df.loc[last, "total_freight"] * 10
    df.loc[last, "customer_lat"] = df.loc[last, "customer_lat"] + 15
    df.loc[last, "order_estimated_delivery_ts_utc"] = approved[last] + pd.Timedelta(days=90)
    df.loc[last, "payment_type"] = "voucher"
    df.loc[last, "product_category"] = "toys"
    df.loc[last, "customer_state"] = "BA"
    df.loc[last, "payment_installments"] = 24
    df.loc[last, "n_items"] = 9
    df.to_parquet(training_path(gold), index=False)


def add_straggler(gold: Path, days_after: int = 5) -> pd.Timestamp:
    """One open order approved `days_after` the last one (the dataset's thin tail)."""
    df = pd.read_parquet(training_path(gold))
    row = df.iloc[[-1]].copy()
    ts = pd.to_datetime(df["order_approved_ts_utc"], utc=True).max() + pd.Timedelta(days_after, "D")
    row["order_id"], row["order_approved_ts_utc"], row["is_late"] = "straggler", ts, pd.NA
    pd.concat([df, row], ignore_index=True).to_parquet(training_path(gold), index=False)
    return ts


def monitoring_rows(gold: Path) -> pd.DataFrame:
    return pd.read_parquet(gold / "ml" / "ml_monitoring.parquet")


def run(env: dict[str, Any]) -> int:
    store: FeatureStore = env["store"]
    return monitor_late_delivery(load_late_delivery_config(), store, MlflowClient())


def metric(rows: pd.DataFrame, name: str) -> pd.Series:
    return rows.loc[rows["metric"] == name, "value"]


def window_of(rows: pd.DataFrame) -> tuple[pd.Timestamp, pd.Timestamp]:
    start, end = rows.loc[rows["metric"] == "drift_share", "window"].item().split("/")
    return pd.Timestamp(start, tz="UTC"), pd.Timestamp(end, tz="UTC")


def test_anchor_skips_a_straggler_tail() -> None:
    days = pd.date_range("2018-07-01", periods=40, freq="D", tz="UTC")
    approved = pd.Series(
        [*days.repeat(100), days[-1] + pd.Timedelta(days=1)] * 1
        + [days[-1] + pd.Timedelta(days=1, hours=h) for h in range(1, 10)]  # 10 on the next day
        + [days[-1] + pd.Timedelta(days=6)]  # a lone straggler after a gap
    )
    assert monitor.anchor_date(approved, tail_ratio=0.2, window_days=28) == days[-1]


def test_anchor_keeps_a_quiet_but_not_thin_last_day() -> None:
    days = pd.date_range("2018-07-01", periods=40, freq="D", tz="UTC")
    approved = pd.Series([*days[:-1].repeat(100), *[days[-1]] * 30])
    assert monitor.anchor_date(approved, tail_ratio=0.2, window_days=28) == days[-1]


def test_unshifted_data_and_good_model_exit_0_with_qualifying_weeks(
    env: dict[str, Any], monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    joined: list[int] = []
    original = monitor.historical_seller_features

    def spy(store: FeatureStore, orders: pd.DataFrame) -> pd.DataFrame:
        joined.append(len(orders))
        return original(store, orders)

    monkeypatch.setattr(monitor, "historical_seller_features", spy)
    monkeypatch.setenv("REFERENCE_SAMPLE_ROWS", "1000")
    monkeypatch.setenv("MIN_LABELLED", "20")
    monkeypatch.setenv("MIN_POSITIVES", "3")
    register_oracle(env["gold"], good_model=True)
    straggler = add_straggler(env["gold"])
    with caplog.at_level(logging.WARNING):
        assert run(env) == 0
    assert "no supported delayed ground-truth week" not in caplog.text
    assert 1000 in joined  # reference sampled before the Feast join
    rows = monitoring_rows(env["gold"])
    assert list(rows.columns) == COLUMNS
    start, anchor = window_of(rows)
    assert anchor < straggler.normalize()
    assert anchor - start == pd.Timedelta(days=27)
    df = pd.read_parquet(training_path(env["gold"]))
    day = pd.to_datetime(df["order_approved_ts_utc"], utc=True).dt.normalize()
    n_current = int(day.between(start, anchor).sum())
    assert n_current in joined
    assert metric(rows, "n_current").item() == n_current
    assert metric(rows, "report_uploaded").item() == 1.0
    assert (env["reports"] / "late_delivery" / f"{anchor.date()}.html").stat().st_size > 0
    metrics = dict(zip(rows["metric"], rows["value"], strict=False))
    assert metrics["drift_share"] <= 0.3
    assert {"prediction_drift", "precision", "recall", "pr_auc"} <= set(metrics)
    assert metric(rows, "pr_auc").min() == pytest.approx(1.0)
    weeks = rows.loc[rows["metric"] == "pr_auc", "window"]
    assert len(weeks) >= 4
    assert weeks.str[:10].min() >= str((start - pd.Timedelta(days=6)).date())


def test_shifted_features_exit_3_via_cli(
    env: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    shift_tail(env["gold"])
    monkeypatch.setattr(cli, "feature_store", lambda: env["store"])
    assert cli.main(["monitor", "late_delivery"]) == BREACH == 3
    rows = monitoring_rows(env["gold"])
    assert rows.loc[rows["metric"] == "drift_share", "value"].item() > 0.3


def test_degraded_model_on_labelled_weeks_exit_3(
    env: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MIN_LABELLED", "20")
    monkeypatch.setenv("MIN_POSITIVES", "3")
    register_oracle(env["gold"], good_model=False)
    assert run(env) == 3
    rows = monitoring_rows(env["gold"])
    assert rows.loc[rows["metric"] == "drift_share", "value"].item() <= 0.3
    assert metric(rows, "pr_auc").iloc[-1] < 0.9 - 0.05


def test_tiny_noisy_weeks_are_recorded_but_do_not_breach(
    env: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    register_oracle(env["gold"], good_model=False)  # ~40 labelled per week < MIN_LABELLED 100
    with caplog.at_level(logging.WARNING):
        assert run(env) == 0
    assert "below support" in caplog.text
    rows = monitoring_rows(env["gold"])
    assert (metric(rows, "pr_auc") < 0.85).all()
    assert len(metric(rows, "n_positives")) == len(metric(rows, "n_labelled")) >= 4


def test_small_current_window_warns_and_does_not_breach_on_drift(
    env: dict[str, Any], monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("MONITOR_WINDOW_DAYS", "2")
    shift_tail(env["gold"])
    with caplog.at_level(logging.WARNING):
        assert run(env) == 0
    assert "MIN_CURRENT_ROWS" in caplog.text
    rows = monitoring_rows(env["gold"])
    start, anchor = window_of(rows)
    assert anchor - start == pd.Timedelta(days=1)
    assert 0 < metric(rows, "n_current").item() < 30


def test_failed_upload_still_writes_rows_and_exits_3(
    env: dict[str, Any], monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    class FailingS3:
        def put_object(self, **kwargs: Any) -> None:
            raise ClientError({"Error": {"Code": "NoSuchBucket", "Message": "x"}}, "PutObject")

    monkeypatch.setenv("MONITORING_REPORT_ROOT", "s3://lakehouse/monitoring")
    monkeypatch.setattr(drift.boto3, "client", lambda *a, **k: FailingS3())
    shift_tail(env["gold"])
    with caplog.at_level(logging.WARNING):
        assert run(env) == 3
    assert "upload" in caplog.text
    rows = monitoring_rows(env["gold"])
    assert metric(rows, "drift_share").item() > 0.3
    assert metric(rows, "report_uploaded").item() == 0.0


def test_empty_training_data_exit_0_with_warning(
    env: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    df = pd.read_parquet(training_path(env["gold"]))
    df.iloc[:0].to_parquet(training_path(env["gold"]), index=False)
    with caplog.at_level(logging.WARNING):
        assert run(env) == 0
    assert "empty current window" in caplog.text
    assert not env["reports"].exists()


def test_does_not_read_the_open_order_predictions(env: dict[str, Any]) -> None:
    (env["gold"] / "ml" / "pred_late_delivery.parquet").write_bytes(b"not parquet")
    assert run(env) == 0


def test_rows_are_appended_across_runs(env: dict[str, Any]) -> None:
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
