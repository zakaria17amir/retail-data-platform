"""Gold readers, data hash and the Feast store (offline joins, materialisation)."""

from __future__ import annotations

import hashlib
import importlib.util
from datetime import timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from feast import Entity, FeatureStore, FeatureView, FileSource, RepoConfig

from retail_ml import config
from retail_ml.features import SELLER_FEATURES

SELLER_VIEW = "seller_stats"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def read_training(path: Path) -> pd.DataFrame:
    """Labelled (delivered) rows of the late-delivery contract."""
    df = pd.read_parquet(path)
    df = df[df["is_late"].notna()].reset_index(drop=True)
    df["is_late"] = df["is_late"].astype(bool)
    return df


def feature_store(repo: Path | None = None) -> FeatureStore:
    """Store from `feature_store.yaml` with REDIS_URL / FEAST_REGISTRY_PATH defaults applied."""
    repo = repo or config.feast_repo_path()
    values = {
        "REDIS_URL": config.redis_connection(),
        "FEAST_REGISTRY_PATH": config.feast_registry_path().as_posix(),
    }
    text = (repo / "feature_store.yaml").read_text(encoding="utf-8")
    for key, value in values.items():
        text = text.replace("${" + key + "}", value)
    config.feast_registry_path().parent.mkdir(parents=True, exist_ok=True)
    return FeatureStore(
        repo_path=str(repo), config=RepoConfig(repo_path=repo, **yaml.safe_load(text))
    )


def apply_feature_definitions(store: FeatureStore, repo: Path) -> None:
    """Import the repo's definitions (GOLD_DIR is read at import) and apply them to the registry."""
    spec = importlib.util.spec_from_file_location("feature_repo_features", repo / "features.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    objects: list[Any] = [
        v for v in vars(module).values() if isinstance(v, Entity | FileSource | FeatureView)
    ]
    store.apply(objects)


def historical_seller_features(store: FeatureStore, orders: pd.DataFrame) -> pd.DataFrame:
    """Point-in-time seller features as of each order's approval; row order preserved."""
    entity = pd.DataFrame(
        {
            "order_id": orders["order_id"].to_numpy(),
            "seller_id": orders["seller_id"].astype(str).to_numpy(),
            "event_timestamp": pd.to_datetime(
                orders["order_approved_ts_utc"], utc=True
            ).reset_index(drop=True),
        }
    )
    joined = store.get_historical_features(
        entity_df=entity, features=[f"{SELLER_VIEW}:{c}" for c in SELLER_FEATURES]
    ).to_df()
    joined = joined.drop_duplicates("order_id").set_index("order_id")
    out = joined.reindex(orders["order_id"])[SELLER_FEATURES].astype("float64")
    return out.reset_index()


def materialize(store: FeatureStore, source: Path) -> None:
    """Load the latest snapshot per seller into the online store over the data's own time span."""
    ts = pd.read_parquet(source, columns=["feature_ts"])["feature_ts"]
    start = pd.to_datetime(ts.min(), utc=True).to_pydatetime()
    end = pd.to_datetime(ts.max(), utc=True).to_pydatetime() + timedelta(seconds=1)
    store.materialize(start_date=start, end_date=end, feature_views=[SELLER_VIEW])
