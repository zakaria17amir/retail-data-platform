"""Champion from the MLflow registry and Feast online seller features (registry read-only)."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

import mlflow
import pandas as pd
from feast import FeatureStore
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

from retail_ml import config
from retail_ml.data import SELLER_VIEW, feature_store
from retail_ml.features import SELLER_FEATURES
from retail_ml.late_delivery.promote import CHAMPION

MODEL_NAME = "late_delivery"
FEATURE_REFS = [f"{SELLER_VIEW}:{c}" for c in SELLER_FEATURES]

SellerLookup = Callable[[str], dict[str, Any]]


class Predictor(Protocol):
    def predict(self, data: pd.DataFrame) -> Any: ...


@dataclass(frozen=True)
class LoadedModel:
    version: str
    model: Predictor


class FeatureStoreUnavailable(RuntimeError):
    pass


def load_champion(name: str = MODEL_NAME) -> LoadedModel | None:
    """None when the model or its `champion` alias doesn't exist yet; other MLflow errors propagate.

    Loads the version the alias resolved to, so the reported version is the one served even if
    the alias moves in between."""
    try:
        aliases = MlflowClient().get_registered_model(name).aliases
    except MlflowException as e:
        if e.error_code == "RESOURCE_DOES_NOT_EXIST":
            return None
        raise
    if CHAMPION not in aliases:
        return None
    version = str(aliases[CHAMPION])
    return LoadedModel(version, mlflow.pyfunc.load_model(f"models:/{name}/{version}"))


def seller_lookup(store: FeatureStore) -> SellerLookup:
    """seller_id → the three `seller_*_90d` values; an unseen seller gets None for each."""

    def lookup(seller_id: str) -> dict[str, Any]:
        row = store.get_online_features(
            features=FEATURE_REFS, entity_rows=[{"seller_id": seller_id}]
        ).to_dict()
        return {c: row[c][0] for c in SELLER_FEATURES}

    return lookup


def load_seller_lookup() -> SellerLookup:
    # Feast creates (writes) a registry it can't find; serving mounts it read-only, so check first.
    registry = config.feast_registry_path()
    if not registry.is_file():
        raise FeatureStoreUnavailable(
            f"Feast registry {registry} not found; run `retail-ml materialize`"
        )
    return seller_lookup(feature_store())
