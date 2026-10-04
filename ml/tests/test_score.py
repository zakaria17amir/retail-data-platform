from pathlib import Path
from typing import Any

import lightgbm as lgb
import mlflow
import numpy as np
import pandas as pd
import pytest
from conftest import local_store, synthetic_gold
from mlflow import MlflowClient
from sklearn.dummy import DummyClassifier
from sklearn.pipeline import Pipeline

import retail_ml.features
from retail_ml.data import historical_seller_features, read_training
from retail_ml.features import SELLER_FEATURES, OrderFeatures
from retail_ml.late_delivery.promote import approve
from retail_ml.late_delivery.score import OUTPUT_COLUMNS, open_orders, score
from retail_ml.late_delivery.train import ProbabilityModel, model_inputs

NAME = "late_delivery"


def _register(pipeline: Pipeline) -> str:
    mlflow.set_experiment("score_test")  # module DB; mlflow_uri resets this per test
    with mlflow.start_run():
        info = mlflow.pyfunc.log_model(
            name="model", python_model=ProbabilityModel(pipeline), pip_requirements=["mlflow"]
        )
    return str(mlflow.register_model(info.model_uri, NAME).version)


def _with_seller(store: Any, df: pd.DataFrame) -> pd.DataFrame:
    seller = historical_seller_features(store, df)
    return model_inputs(pd.concat([df, seller[SELLER_FEATURES]], axis=1))


def test_score_without_champion_fails_clearly(mlflow_uri: str, gold_dir: Path) -> None:
    with pytest.raises(RuntimeError, match="no champion"):
        score(None, MlflowClient(), gold_dir / "missing.parquet", NAME)  # type: ignore[arg-type]


@pytest.fixture(scope="module")
def setup(tmp_path_factory: pytest.TempPathFactory) -> Any:
    tmp_path = tmp_path_factory.mktemp("score")
    mp = pytest.MonkeyPatch()
    gold_dir = tmp_path / "gold"
    mp.setenv("GOLD_DIR", str(gold_dir))
    mp.chdir(tmp_path)
    uri = f"sqlite:///{(tmp_path / 'mlflow.db').as_posix()}"
    mp.setenv("MLFLOW_TRACKING_URI", uri)
    mlflow.set_tracking_uri(uri)
    synthetic_gold(gold_dir, n_orders=800)
    path = gold_dir / "ml" / "late_delivery_training.parquet"
    raw = pd.read_parquet(path)
    open_ids = raw.loc[raw["order_delivered_customer_ts_utc"].isna(), "order_id"].tolist()
    raw.loc[raw["order_id"] == open_ids[0], "order_status"] = "canceled"
    raw.loc[raw["order_id"] == open_ids[1], "order_status"] = "unavailable"
    raw.loc[raw["order_id"] == open_ids[2], "order_status"] = "delivered"  # no delivery ts
    raw.to_parquet(path, index=False)
    store = local_store(tmp_path)
    labelled = read_training(path)
    X, y = _with_seller(store, labelled), labelled["is_late"].astype(int)
    lgbm = Pipeline(
        [
            ("features", OrderFeatures()),
            ("model", lgb.LGBMClassifier(n_estimators=30, verbose=-1, random_state=0)),
        ]
    ).fit(X, y)
    champion = _register(lgbm)
    approve(MlflowClient(), NAME, champion)
    constant = Pipeline([("model", DummyClassifier(strategy="prior"))]).fit(X, y)
    challenger = _register(constant)
    MlflowClient().set_registered_model_alias(NAME, "challenger", challenger)
    yield {
        "store": store,
        "path": path,
        "raw": raw,
        "lgbm": lgbm,
        "champion": champion,
        "excluded": set(open_ids[:3]),
    }
    mp.undo()


def test_open_orders_are_approved_undelivered_and_not_canceled(setup: dict[str, Any]) -> None:
    raw = setup["raw"]
    ids = set(open_orders(raw)["order_id"])
    undelivered = set(raw.loc[raw["order_delivered_customer_ts_utc"].isna(), "order_id"])
    assert ids == undelivered - setup["excluded"]
    assert len(ids) > 10


def test_score_uses_the_champion_and_order_features(
    setup: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[int] = []
    original = retail_ml.features.order_features

    def spy(df: pd.DataFrame, vocabulary: Any = None) -> pd.DataFrame:
        calls.append(len(df))
        return original(df, vocabulary)

    monkeypatch.setattr(retail_ml.features, "order_features", spy)
    now = pd.Timestamp("2018-09-03 06:00", tz="UTC")
    out = score(setup["store"], MlflowClient(), setup["path"], NAME, now=now)
    expected_orders = open_orders(setup["raw"])
    assert list(out.columns) == OUTPUT_COLUMNS
    assert out["order_id"].tolist() == expected_orders["order_id"].tolist()
    assert set(out["model_version"]) == {setup["champion"]}
    assert (out["scored_ts"] == now).all()
    assert calls == [len(expected_orders)]
    monkeypatch.setattr(retail_ml.features, "order_features", original)
    expected = setup["lgbm"].predict_proba(_with_seller(setup["store"], expected_orders))[:, 1]
    np.testing.assert_allclose(out["probability"].to_numpy(), expected)
    assert out["probability"].nunique() > 1  # not the constant challenger


def test_seller_features_are_as_of_each_orders_approval(
    setup: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[pd.DataFrame] = []
    original = ProbabilityModel.predict

    def capture(self: Any, context: Any, model_input: pd.DataFrame, params: Any = None) -> Any:
        seen.append(model_input.copy())
        return original(self, context, model_input, params)

    monkeypatch.setattr(ProbabilityModel, "predict", capture)
    score(setup["store"], MlflowClient(), setup["path"], NAME)
    orders = open_orders(setup["raw"])
    snap = pd.read_parquet(setup["path"].parent / "seller_features_daily.parquet")
    snap["feature_ts"] = pd.to_datetime(snap["feature_ts"], utc=True)
    row = orders.iloc[0]
    approved = pd.Timestamp(row["order_approved_ts_utc"])
    asof = snap[(snap["seller_id"] == row["seller_id"]) & (snap["feature_ts"] <= approved)]
    expected = asof.sort_values("feature_ts").iloc[-1]["seller_late_rate_90d"]
    assert seen[0]["seller_late_rate_90d"].iloc[0] == pytest.approx(expected)


def test_no_open_orders_writes_an_empty_frame(setup: dict[str, Any]) -> None:
    raw = setup["raw"]
    path = setup["path"].with_name("all_delivered.parquet")
    raw[raw["order_delivered_customer_ts_utc"].notna()].to_parquet(path, index=False)
    out = score(setup["store"], MlflowClient(), path, NAME)
    assert out.empty and list(out.columns) == OUTPUT_COLUMNS
