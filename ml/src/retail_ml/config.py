"""Environment settings (documented defaults) and the YAML training config."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "configs" / "late_delivery.yaml"


def gold_dir() -> Path:
    return Path(os.environ.get("GOLD_DIR") or "data/gold")


def feast_repo_path() -> Path:
    return Path(
        os.environ.get("FEAST_REPO_PATH") or Path(__file__).resolve().parents[2] / "feature_repo"
    )


def feast_registry_path() -> Path:
    return Path(os.environ.get("FEAST_REGISTRY_PATH") or "data/feast/registry.db").resolve()


def redis_connection() -> str:
    """Feast wants `host:port[,opts]`; accept a `redis://host:port` URL too."""
    return (os.environ.get("REDIS_URL") or "localhost:6379").removeprefix("redis://")


def promotion_mode() -> str:
    return os.environ.get("PROMOTION_MODE") or "auto"


@dataclass(frozen=True)
class LateDeliveryConfig:
    experiment: str
    registered_model: str
    training_path: Path
    validation_start: pd.Timestamp
    test_start: pd.Timestamp
    test_end: pd.Timestamp
    logistic: dict[str, Any]
    lightgbm: dict[str, Any]
    early_stopping_rounds: int


def load_late_delivery_config(path: Path | None = None) -> LateDeliveryConfig:
    raw = yaml.safe_load((path or DEFAULT_CONFIG).read_text(encoding="utf-8"))
    split = raw["split"]
    return LateDeliveryConfig(
        experiment=raw["experiment"],
        registered_model=raw["registered_model"],
        training_path=gold_dir() / raw["training_path"],
        validation_start=pd.Timestamp(split["validation_start"], tz="UTC"),
        test_start=pd.Timestamp(split["test_start"], tz="UTC"),
        test_end=pd.Timestamp(split["test_end"], tz="UTC"),
        logistic=dict(raw["logistic"]),
        lightgbm=dict(raw["lightgbm"]),
        early_stopping_rounds=int(raw["early_stopping_rounds"]),
    )
