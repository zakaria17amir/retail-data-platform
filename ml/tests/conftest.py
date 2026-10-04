from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from feast import FeatureStore, RepoConfig

from retail_ml.data import apply_feature_definitions

FEATURE_REPO = Path(__file__).resolve().parents[1] / "feature_repo"
CATEGORIES = ["bed_bath_table", "health_beauty", "sports_leisure", "furniture_decor", "toys"]
PAYMENTS = ["credit_card", "boleto", "voucher", "debit_card"]
STATES = ["SP", "RJ", "MG", "RS", "BA"]


def raw_order(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "order_id": "o1",
        "customer_id": "c1",
        "customer_unique_id": "u1",
        "seller_id": "s1",
        "order_status": "delivered",
        "order_purchase_ts_utc": pd.Timestamp("2018-01-10 09:00", tz="UTC"),
        "order_approved_ts_utc": pd.Timestamp("2018-01-10 10:30", tz="UTC"),
        "order_estimated_delivery_ts_utc": pd.Timestamp("2018-01-25 00:00", tz="UTC"),
        "order_delivered_customer_ts_utc": pd.Timestamp("2018-01-20 12:00", tz="UTC"),
        "is_late": False,
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
    row.update(overrides)
    return row


def synthetic_gold(
    gold_dir: Path, n_orders: int = 4000, n_sellers: int = 60, seed: int = 7
) -> None:
    """Contract-shaped training + seller snapshot Parquet with a planted late-delivery signal."""
    rng = np.random.default_rng(seed)
    sellers = [f"s{i:03d}" for i in range(n_sellers)]
    propensity = dict(zip(sellers, rng.uniform(0.0, 0.6, n_sellers), strict=True))
    seller_state = dict(zip(sellers, rng.choice(STATES, n_sellers), strict=True))

    days = pd.date_range("2016-12-01", "2018-09-01", freq="D", tz="UTC")
    snap = pd.DataFrame(
        [(s, d) for s in sellers for d in days], columns=["seller_id", "feature_ts"]
    )
    snap["seller_orders_90d"] = rng.integers(1, 60, len(snap)).astype("int64")
    snap["seller_late_rate_90d"] = (
        snap["seller_id"].map(propensity) + rng.normal(0, 0.03, len(snap))
    ).clip(0, 1)
    snap["seller_avg_delivery_days_90d"] = rng.uniform(5, 20, len(snap))
    snap["created_ts"] = snap["feature_ts"] + pd.Timedelta(hours=2)

    approved = pd.Timestamp("2017-01-01", tz="UTC") + pd.to_timedelta(
        rng.uniform(0, 607, n_orders), unit="D"
    ).floor("s")
    seller = rng.choice(sellers, n_orders)
    est_days = rng.uniform(5, 40, n_orders)
    c_lat, c_lng = rng.uniform(-30, -5, n_orders), rng.uniform(-55, -35, n_orders)
    s_lat, s_lng = rng.uniform(-30, -5, n_orders), rng.uniform(-55, -35, n_orders)
    dist = np.hypot(c_lat - s_lat, c_lng - s_lng)
    prop = pd.Series(seller).map(propensity).to_numpy()
    # non-linear on purpose: late when the promised window is short for the distance
    logit = -5.0 + 6.0 * prop + 4.0 * (dist / est_days > 0.6)
    is_late = rng.uniform(size=n_orders) < 1 / (1 + np.exp(-logit))
    delivered = rng.uniform(size=n_orders) > 0.05

    df = pd.DataFrame(
        {
            "order_id": [f"o{i:05d}" for i in range(n_orders)],
            "customer_id": [f"c{i:05d}" for i in range(n_orders)],
            "customer_unique_id": [f"u{i:05d}" for i in range(n_orders)],
            "seller_id": seller,
            "order_status": np.where(delivered, "delivered", "shipped"),
            "order_purchase_ts_utc": approved - pd.Timedelta(hours=1),
            "order_approved_ts_utc": approved,
            "order_estimated_delivery_ts_utc": approved + pd.to_timedelta(est_days, unit="D"),
            "order_delivered_customer_ts_utc": pd.Series(
                approved + pd.to_timedelta(est_days + np.where(is_late, 3, -3), unit="D")
            ).where(delivered),
            "is_late": pd.Series(is_late, dtype="boolean").where(delivered),
            "n_items": rng.integers(1, 4, n_orders),
            "n_sellers": 1,
            "total_price": rng.uniform(10, 500, n_orders),
            "total_freight": rng.uniform(5, 60, n_orders),
            "product_category": rng.choice(CATEGORIES, n_orders),
            "payment_type": rng.choice(PAYMENTS, n_orders),
            "payment_installments": rng.integers(1, 10, n_orders),
            "customer_state": rng.choice(STATES, n_orders),
            "customer_lat": c_lat,
            "customer_lng": c_lng,
            "seller_state": pd.Series(seller).map(seller_state),
            "seller_lat": s_lat,
            "seller_lng": s_lng,
        }
    )
    (gold_dir / "ml").mkdir(parents=True, exist_ok=True)
    df.to_parquet(gold_dir / "ml" / "late_delivery_training.parquet", index=False)
    snap.to_parquet(gold_dir / "ml" / "seller_features_daily.parquet", index=False)


def local_store(tmp_path: Path) -> FeatureStore:
    """Feast store over the committed definitions: file offline, SQLite online, tmp registry."""
    config = RepoConfig(
        project="retail",
        provider="local",
        registry=str(tmp_path / "feast" / "registry.db"),
        online_store={"type": "sqlite", "path": str(tmp_path / "feast" / "online.db")},
        offline_store={"type": "file"},
        entity_key_serialization_version=3,
        repo_path=FEATURE_REPO,
    )
    (tmp_path / "feast").mkdir(exist_ok=True)
    store = FeatureStore(repo_path=str(FEATURE_REPO), config=config)
    apply_feature_definitions(store, FEATURE_REPO)
    return store


@pytest.fixture
def gold_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    gold = tmp_path / "gold"
    gold.mkdir()
    monkeypatch.setenv("GOLD_DIR", str(gold))
    return gold


@pytest.fixture
def mlflow_uri(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    import mlflow

    uri = f"sqlite:///{(tmp_path / 'mlflow.db').as_posix()}"
    monkeypatch.chdir(tmp_path)  # default artefact root ./mlruns lands in tmp_path
    monkeypatch.setenv("MLFLOW_TRACKING_URI", uri)
    mlflow.set_tracking_uri(uri)
    return uri
