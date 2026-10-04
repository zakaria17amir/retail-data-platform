from pathlib import Path

import mlflow
import numpy as np
import pandas as pd
import pytest
from conftest import local_store, raw_order, synthetic_gold
from mlflow import MlflowClient

from retail_ml.config import load_late_delivery_config
from retail_ml.data import sha256_file
from retail_ml.features import SELLER_FEATURES
from retail_ml.late_delivery.evaluate import classification_metrics
from retail_ml.late_delivery.train import model_inputs, train

ROC_AUC_FLOOR = 0.80
METRICS = ["pr_auc", "roc_auc", "brier", "recall_at_p50"]


def test_classification_metrics_on_a_known_ranking() -> None:
    y = pd.Series([0, 0, 1, 1])
    m = classification_metrics(y, np.array([0.1, 0.6, 0.4, 0.9]))
    assert m["roc_auc"] == pytest.approx(0.75)
    assert m["pr_auc"] == pytest.approx(0.8333, abs=1e-3)
    assert m["brier"] == pytest.approx((0.01 + 0.36 + 0.36 + 0.01) / 4)
    assert m["recall_at_p50"] == 1.0


@pytest.fixture(scope="module")
def trained(tmp_path_factory: pytest.TempPathFactory) -> dict[str, object]:
    tmp = tmp_path_factory.mktemp("train")
    mp = pytest.MonkeyPatch()
    gold = tmp / "gold"
    mp.setenv("GOLD_DIR", str(gold))
    mp.chdir(tmp)
    uri = f"sqlite:///{(tmp / 'mlflow.db').as_posix()}"
    mp.setenv("MLFLOW_TRACKING_URI", uri)
    mlflow.set_tracking_uri(uri)
    synthetic_gold(gold)
    cfg = load_late_delivery_config()
    result = train(cfg, local_store(tmp), promotion_mode="auto")
    yield {"result": result, "cfg": cfg, "gold": gold}
    mp.undo()


def test_three_runs_logged_baselines_first_with_lineage(trained: dict[str, object]) -> None:
    client = MlflowClient()
    exp = client.get_experiment_by_name("late_delivery")
    assert exp is not None
    runs = client.search_runs([exp.experiment_id], order_by=["attributes.start_time ASC"])
    assert [r.data.tags["model_type"] for r in runs] == ["constant_prior", "logistic", "lightgbm"]
    data_hash = sha256_file(trained["gold"] / "ml" / "late_delivery_training.parquet")  # type: ignore[operator]
    for r in runs:
        assert r.data.tags["data_sha256"] == data_hash
        assert r.data.tags["git_sha"]
        assert {f"{s}_{m}" for s in ("val", "test") for m in METRICS} <= set(r.data.metrics)
        assert int(r.data.params["n_train"]) > 0
    lgbm = runs[2]
    assert int(lgbm.data.params["best_iteration"]) > 0
    assert lgbm.data.metrics["test_roc_auc"] >= ROC_AUC_FLOOR
    assert lgbm.data.metrics["test_pr_auc"] > runs[0].data.metrics["test_pr_auc"]


def test_only_delivered_orders_are_used(trained: dict[str, object]) -> None:
    df = pd.read_parquet(trained["gold"] / "ml" / "late_delivery_training.parquet")  # type: ignore[operator]
    run = MlflowClient().search_runs(
        [MlflowClient().get_experiment_by_name("late_delivery").experiment_id],  # type: ignore[union-attr]
        filter_string="tags.model_type = 'lightgbm'",
    )[0]
    p = run.data.params
    approved = pd.to_datetime(df["order_approved_ts_utc"], utc=True)
    in_window = df["is_late"].notna() & (approved < pd.Timestamp("2018-09-01", tz="UTC"))
    assert int(p["n_train"]) + int(p["n_val"]) + int(p["n_test"]) == int(in_window.sum())


def test_lightgbm_registered_as_challenger_and_first_promoted(trained: dict[str, object]) -> None:
    aliases = {
        k: str(v) for k, v in MlflowClient().get_registered_model("late_delivery").aliases.items()
    }
    assert aliases == {"challenger": "1", "champion": "1"}


def test_registered_model_scores_raw_rows_with_unknowns(trained: dict[str, object]) -> None:
    model = mlflow.pyfunc.load_model("models:/late_delivery@champion")
    rows = pd.DataFrame(
        [
            raw_order(),
            raw_order(product_category="brand_new", payment_type="pix", seller_state="AM"),
            raw_order(payment_type=None, customer_lat=None, n_items=None),
        ]
    )
    for col in SELLER_FEATURES:
        rows[col] = [0.2, np.nan, np.nan]
    p = np.asarray(model.predict(model_inputs(rows)))
    assert p.shape == (3,)
    assert np.isfinite(p).all() and ((p >= 0) & (p <= 1)).all()


def test_config_paths_follow_gold_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GOLD_DIR", str(tmp_path))
    cfg = load_late_delivery_config()
    assert cfg.training_path == tmp_path / "ml" / "late_delivery_training.parquet"
    assert cfg.experiment == cfg.registered_model == "late_delivery"
