from __future__ import annotations

import math
import os
import subprocess
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import lightgbm as lgb
import mlflow
import numpy as np
import pandas as pd
import pytest
from conftest import local_store, synthetic_gold
from fastapi.testclient import TestClient
from mlflow import MlflowClient
from mlflow.models import infer_signature
from prometheus_client.parser import text_string_to_metric_families
from sklearn.pipeline import Pipeline

from retail_ml.data import materialize
from retail_ml.features import SELLER_FEATURES, OrderFeatures
from retail_ml.late_delivery.train import MODEL_INPUT_COLUMNS, ProbabilityModel, model_inputs
from retail_ml.serving.app import create_app
from retail_ml.serving.metrics import PROBABILITY_BUCKETS
from retail_ml.serving.model import (
    FeatureStoreUnavailable,
    LoadedModel,
    load_champion,
    load_seller_lookup,
    seller_lookup,
    single_threaded,
)

PREDICT = "/predict/late-delivery"
PAYLOAD: dict[str, Any] = {
    "order_id": "o1",
    "customer_id": "c1",
    "customer_unique_id": "u1",
    "seller_id": "s1",
    "order_purchase_ts_utc": "2018-07-01T09:00:00Z",
    "order_approved_ts_utc": "2018-07-01T10:30:00Z",
    "order_estimated_delivery_ts_utc": "2018-07-20T00:00:00Z",
    "n_items": 2,
    "n_sellers": 1,
    "total_price": 100.0,
    "total_freight": 20.0,
    "product_category": "toys",
    "payment_type": "credit_card",
    "payment_installments": 3,
    "customer_state": "SP",
    "customer_lat": -23.55,
    "customer_lng": -46.63,
    "seller_state": "RJ",
    "seller_lat": -22.91,
    "seller_lng": -43.17,
}
KNOWN = {
    "seller_orders_90d": 12.0,
    "seller_late_rate_90d": 0.25,
    "seller_avg_delivery_days_90d": 9.5,
}
UNKNOWN: dict[str, float | None] = dict.fromkeys(SELLER_FEATURES)


class StubModel:
    def __init__(self, probability: float = 0.42) -> None:
        self.probability = probability
        self.inputs: list[pd.DataFrame] = []

    def predict(self, df: pd.DataFrame) -> Any:
        self.inputs.append(df)
        return np.full(len(df), self.probability)


def _store(features: dict[str, float | None]) -> Callable[[], Callable[[str], Any]]:
    return lambda: lambda seller_id: dict(features)


@pytest.fixture
def stub() -> StubModel:
    return StubModel()


@pytest.fixture
def client(stub: StubModel) -> Iterator[TestClient]:
    app = create_app(load_model=lambda: LoadedModel("3", stub), load_lookup=_store(KNOWN))
    with TestClient(app) as c:
        yield c


def test_health_is_live_and_predict_503_without_champion() -> None:
    app = create_app(load_model=lambda: None, load_lookup=_store(KNOWN))
    with TestClient(app) as c:
        health = c.get("/health")
        assert health.status_code == 200
        assert health.json() == {"model_loaded": False, "model_version": None}
        r = c.post(PREDICT, json=PAYLOAD)
        assert r.status_code == 503
        assert "no champion" in r.json()["detail"]


def test_health_is_live_when_registry_is_unreachable() -> None:
    def broken() -> LoadedModel | None:
        raise ConnectionError("mlflow down")

    with TestClient(create_app(load_model=broken, load_lookup=_store(KNOWN))) as c:
        assert c.get("/health").json() == {"model_loaded": False, "model_version": None}


def test_predict_contract_shapes_inputs_with_model_inputs(
    client: TestClient, stub: StubModel
) -> None:
    r = client.post(PREDICT, json=PAYLOAD)
    assert r.status_code == 200
    assert r.json() == {"probability": 0.42, "model_version": "3"}
    (X,) = stub.inputs
    assert list(X.columns) == MODEL_INPUT_COLUMNS
    assert X.loc[0, "seller_late_rate_90d"] == 0.25
    expected = model_inputs(pd.DataFrame([{**PAYLOAD, **KNOWN}]))
    pd.testing.assert_frame_equal(X, expected)


@pytest.mark.parametrize(
    "change",
    [
        {"is_late": True},
        {"order_delivered_customer_ts_utc": "2018-07-10T00:00:00Z"},
        {"order_status": "delivered"},
        {"seller_id": None},
        {"n_items": 0},
        {"total_price": -1.0},
        {"customer_lat": 123.0},
    ],
)
def test_request_rejects_labels_post_approval_fields_and_invalid_values(
    client: TestClient, change: dict[str, Any]
) -> None:
    assert client.post(PREDICT, json={**PAYLOAD, **change}).status_code == 422


def test_missing_required_field_is_422(client: TestClient) -> None:
    body = {k: v for k, v in PAYLOAD.items() if k != "order_approved_ts_utc"}
    assert client.post(PREDICT, json=body).status_code == 422


def test_unknown_seller_gets_nan_defaults(stub: StubModel) -> None:
    app = create_app(load_model=lambda: LoadedModel("3", stub), load_lookup=_store(UNKNOWN))
    with TestClient(app) as c:
        assert c.post(PREDICT, json={**PAYLOAD, "seller_id": "never-seen"}).status_code == 200
    (X,) = stub.inputs
    assert X[SELLER_FEATURES].isna().all().all()
    assert (X[SELLER_FEATURES].dtypes == "float64").all()


def test_feature_store_unavailable_is_503_not_500(stub: StubModel) -> None:
    def no_store() -> Callable[[str], Any]:
        raise FeatureStoreUnavailable("registry missing")

    app = create_app(load_model=lambda: LoadedModel("3", stub), load_lookup=no_store)
    with TestClient(app) as c:
        assert c.get("/health").status_code == 200
        r = c.post(PREDICT, json=PAYLOAD)
        assert r.status_code == 503
        assert "feature store" in r.json()["detail"]


def test_feature_store_is_retried_after_a_failed_start(stub: StubModel) -> None:
    calls: list[int] = []

    def flaky() -> Callable[[str], Any]:
        calls.append(1)
        if len(calls) == 1:
            raise FeatureStoreUnavailable("not yet")
        return lambda seller_id: dict(KNOWN)

    app = create_app(load_model=lambda: LoadedModel("3", stub), load_lookup=flaky)
    with TestClient(app) as c:
        assert c.post(PREDICT, json=PAYLOAD).status_code == 200


@pytest.fixture
def real_pyfunc(tmp_path: Path, gold_dir: Path) -> Any:
    """T3's ProbabilityModel (OrderFeatures + LightGBM) saved and reloaded as an MLflow pyfunc,
    so signature enforcement runs exactly as in serving."""
    synthetic_gold(gold_dir, n_orders=600)
    df = pd.read_parquet(gold_dir / "ml" / "late_delivery_training.parquet")
    df = df[df["is_late"].notna()].reset_index(drop=True)
    rng = np.random.default_rng(0)
    for col in SELLER_FEATURES:
        df[col] = rng.uniform(0, 1, len(df))
    df.loc[::7, SELLER_FEATURES] = np.nan  # training also sees sellers without a snapshot
    X = model_inputs(df)
    pipeline = Pipeline(
        [("features", OrderFeatures()), ("model", lgb.LGBMClassifier(n_estimators=20, verbose=-1))]
    ).fit(X, df["is_late"].astype(int))
    model = ProbabilityModel(pipeline)
    path = tmp_path / "pyfunc"
    mlflow.pyfunc.save_model(
        path,
        python_model=model,
        signature=infer_signature(X.head(5), model.predict(None, X.head(5))),
    )
    return mlflow.pyfunc.load_model(str(path))


def test_unknown_seller_and_categories_with_real_pyfunc(real_pyfunc: Any) -> None:
    app = create_app(load_model=lambda: LoadedModel("1", real_pyfunc), load_lookup=_store(UNKNOWN))
    body = {
        **PAYLOAD,
        "seller_id": "never-seen",
        "product_category": "brand_new_category",
        "payment_type": None,
        "customer_state": "ZZ",
        "customer_lat": None,
        "customer_lng": None,
    }
    with TestClient(app) as c:
        r = c.post(PREDICT, json=body)
    assert r.status_code == 200, r.text
    p = r.json()["probability"]
    assert math.isfinite(p) and 0.0 <= p <= 1.0


def _samples(text: str) -> dict[tuple[str, frozenset[tuple[str, str]]], float]:
    return {
        (s.name, frozenset(s.labels.items())): s.value
        for family in text_string_to_metric_families(text)
        for s in family.samples
    }


def test_metrics_exposed_with_contract_names(client: TestClient) -> None:
    # multiprocess (mmap) values are shared by every app in this process, so assert deltas
    # (after one warm-up call creates each labelled child)
    def calls() -> None:
        client.post(PREDICT, json=PAYLOAD)
        client.post(PREDICT, json={**PAYLOAD, "is_late": True})

    calls()
    before = _samples(client.get("/metrics").text)
    calls()
    r = client.get("/metrics")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/plain")
    text = r.text
    after = _samples(text)

    def delta(name: str, **labels: str) -> float:
        key = (name, frozenset(labels.items()))
        return after[key] - before.get(key, 0.0)

    route = "/predict/late-delivery"
    assert delta("requests_total", route=route, status="200") == 1
    assert delta("requests_total", route=route, status="422") == 1
    assert "# TYPE request_latency_seconds histogram" in text
    assert delta("request_latency_seconds_count", route=route) == 2
    assert "# TYPE prediction_probability histogram" in text
    assert delta("prediction_probability_bucket", le="0.4") == 0
    assert delta("prediction_probability_bucket", le="0.45") == 1
    assert "# TYPE model_version_info gauge" in text
    assert 'model_version_info{version="3"} 1.0' in text
    les = [
        line.split('le="')[1].split('"')[0]
        for line in text.splitlines()
        if line.startswith("prediction_probability_bucket")
    ]
    assert les == [*(repr(b) for b in PROBABILITY_BUCKETS), "+Inf"]
    assert PROBABILITY_BUCKETS[0] == 0.05 and PROBABILITY_BUCKETS[-1] == 1.0
    assert len(PROBABILITY_BUCKETS) == 20


def test_reload_swaps_version() -> None:
    models = iter([LoadedModel("1", StubModel(0.1)), LoadedModel("2", StubModel(0.9))])
    app = create_app(load_model=lambda: next(models), load_lookup=_store(KNOWN))
    with TestClient(app) as c:
        assert c.post(PREDICT, json=PAYLOAD).json() == {"probability": 0.1, "model_version": "1"}
        r = c.post("/reload")
        assert r.status_code == 200
        assert r.json() == {"model_loaded": True, "model_version": "2"}
        assert c.get("/health").json() == {"model_loaded": True, "model_version": "2"}
        assert c.post(PREDICT, json=PAYLOAD).json() == {"probability": 0.9, "model_version": "2"}
        text = c.get("/metrics").text
    assert 'model_version_info{version="2"} 1.0' in text
    assert 'model_version_info{version="1"} 0.0' in text


def test_metrics_aggregate_other_worker_processes(client: TestClient) -> None:
    # uvicorn --workers: each worker writes its own mmap files; /metrics must sum all of them
    def other_workers() -> float:
        key = ("requests_total", frozenset({("route", "/other-worker"), ("status", "200")}))
        return _samples(client.get("/metrics").text).get(key, 0.0)

    before = other_workers()
    worker = (
        "from prometheus_client import Counter, Gauge;"
        "Counter('requests_total', 'h', ['route', 'status']).labels('/other-worker', '200').inc(5);"
        "Gauge('model_version_info', 'h', ['version'], multiprocess_mode='max')"
        ".labels('other').set(1)"
    )
    subprocess.run([sys.executable, "-c", worker], check=True, env=os.environ)
    assert other_workers() - before == 5
    assert 'model_version_info{version="other"} 1.0' in client.get("/metrics").text


def test_champion_predicts_single_threaded_with_unchanged_output(real_pyfunc: Any) -> None:
    lgbm = real_pyfunc.unwrap_python_model().pipeline.named_steps["model"]
    X = model_inputs(pd.DataFrame([{**PAYLOAD, **KNOWN}]))
    expected = real_pyfunc.predict(X)
    assert single_threaded(real_pyfunc) is real_pyfunc
    assert lgbm.get_params()["n_jobs"] == 1
    np.testing.assert_array_equal(real_pyfunc.predict(X), expected)


def test_reload_failure_keeps_serving_current_model() -> None:
    def second_call_fails() -> Iterator[LoadedModel | None]:
        yield LoadedModel("1", StubModel())
        yield None

    models = second_call_fails()
    app = create_app(load_model=lambda: next(models), load_lookup=_store(KNOWN))
    with TestClient(app) as c:
        r = c.post("/reload")
        assert r.status_code == 503
        assert "no champion" in r.json()["detail"]
        assert c.get("/health").json() == {"model_loaded": True, "model_version": "1"}


def test_load_champion_reads_the_registry_alias(mlflow_uri: str) -> None:
    assert load_champion() is None  # registered model does not exist yet
    with mlflow.start_run():
        info = mlflow.pyfunc.log_model(name="model", python_model=ProbabilityModel(None))  # type: ignore[arg-type]
    mlflow.register_model(info.model_uri, "late_delivery")
    mlflow.register_model(info.model_uri, "late_delivery")
    assert load_champion() is None  # registered, but no champion alias yet
    MlflowClient().set_registered_model_alias("late_delivery", "champion", "1")
    loaded = load_champion()
    assert loaded is not None and loaded.version == "1"


def test_seller_lookup_reads_online_store_and_misses_unknown(
    gold_dir: Path, tmp_path: Path
) -> None:
    snap = pd.DataFrame(
        {
            "seller_id": ["s1", "s1"],
            "feature_ts": pd.to_datetime(["2018-01-09", "2018-01-10"], utc=True),
            "seller_orders_90d": [1, 3],
            "seller_late_rate_90d": [0.1, 0.25],
            "seller_avg_delivery_days_90d": [5.0, 6.5],
            "created_ts": pd.to_datetime(["2018-01-09 02:00", "2018-01-10 02:00"], utc=True),
        }
    )
    (gold_dir / "ml").mkdir(parents=True)
    snap.to_parquet(gold_dir / "ml" / "seller_features_daily.parquet", index=False)
    store = local_store(tmp_path)
    materialize(store, gold_dir / "ml" / "seller_features_daily.parquet")
    lookup = seller_lookup(store)
    assert lookup("s1") == {
        "seller_orders_90d": 3,
        "seller_late_rate_90d": 0.25,
        "seller_avg_delivery_days_90d": 6.5,
    }
    assert lookup("never-seen") == UNKNOWN


def test_load_seller_lookup_never_creates_a_missing_registry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = tmp_path / "feast" / "registry.db"
    monkeypatch.setenv("FEAST_REGISTRY_PATH", str(registry))
    with pytest.raises(FeatureStoreUnavailable):
        load_seller_lookup()
    assert not registry.exists() and not registry.parent.exists()
