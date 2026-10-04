"""Environment settings (documented defaults) and the YAML training config."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

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
    """REDIS_URL (`redis[s]://[user:pass@]host[:port][/db]` or Feast `host:port[,opts]`) as the
    Feast connection string `host:port[,db=N][,ssl=true][,username=…][,password=…]`."""
    raw = os.environ.get("REDIS_URL") or "localhost:6379"
    if "://" not in raw:
        return raw
    url = urlsplit(raw)
    parts = [f"{url.hostname or 'localhost'}:{url.port or 6379}"]
    if db := url.path.strip("/"):
        parts.append(f"db={int(db)}")
    if url.scheme == "rediss":
        parts.append("ssl=true")
    if url.username:
        parts.append(f"username={unquote(url.username)}")
    if url.password:
        password = unquote(url.password)
        if "," in password:
            raise ValueError("REDIS_URL password must not contain ',' (Feast option separator)")
        parts.append(f"password={password}")
    return ",".join(parts)


PROMOTION_MODES = ("auto", "manual")


def check_promotion_mode(mode: str) -> str:
    mode = mode.strip().lower()
    if mode not in PROMOTION_MODES:
        raise ValueError(f"promotion mode must be one of {PROMOTION_MODES}, got {mode!r}")
    return mode


def promotion_mode() -> str:
    raw = os.environ.get("PROMOTION_MODE") or "auto"
    try:
        return check_promotion_mode(raw)
    except ValueError as e:
        raise ValueError(f"PROMOTION_MODE: {e}") from None


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
