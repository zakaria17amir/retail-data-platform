"""`retail-ml publish-candidates`: the champion run's candidate lists + popularity -> Redis.

Keys (plan data contract): `cand:<product_id>` -> JSON `[[product_id, score, source], ...]`
(<= n_candidates); `pop:global`, `pop:category:<category>` -> JSON `[product_id, ...]`."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

import mlflow
import pandas as pd
from mlflow import MlflowClient

from retail_ml import config
from retail_ml.late_delivery.promote import champion_version
from retail_ml.recommender.candidates import Popularity

ARTIFACT_DIR = "candidates"
LISTS_FILE, POPULARITY_FILE = "candidates.parquet", "popularity.json"
PREFIXES = ("cand:*", "pop:*")


def candidate_payloads(lists: pd.DataFrame, pop: Popularity) -> dict[str, str]:
    out = {
        f"cand:{anchor}": json.dumps(
            [
                [c, round(float(s), 6), src]
                for c, s, src in zip(g["candidate"], g["score"], g["source"], strict=True)
            ]
        )
        for anchor, g in lists.groupby("anchor", sort=False, observed=True)
    }
    out["pop:global"] = json.dumps(pop["global"])
    for category, ids in pop["category"].items():
        out[f"pop:category:{category}"] = json.dumps(ids)
    return out


def publish(client: Any, payloads: dict[str, str], batch: int = 1000) -> dict[str, int]:
    """Write every key (non-transactional pipeline, `batch` per round trip), then delete
    `cand:*` / `pop:*` keys the new champion no longer has. Other keys (Feast) are untouched."""
    pipe = client.pipeline(transaction=False)
    for i, (key, value) in enumerate(payloads.items(), 1):
        pipe.set(key, value)
        if i % batch == 0:
            pipe.execute()
    pipe.execute()
    stale = [
        k for p in PREFIXES for k in client.scan_iter(match=p, count=1000) if k not in payloads
    ]
    for i in range(0, len(stale), batch):
        client.delete(*stale[i : i + batch])
    n_cand = sum(k.startswith("cand:") for k in payloads)
    return {
        "candidate_keys": n_cand,
        "popularity_keys": len(payloads) - n_cand,
        "stale_deleted": len(stale),
    }


def log_candidates(lists: pd.DataFrame, pop: Popularity) -> None:
    """Inside the active run: the artefact `publish-candidates` reads back."""
    with tempfile.TemporaryDirectory() as tmp:
        ids = {"anchor": "category", "candidate": "category"}  # dictionary-encoded: no copies
        lists.astype(ids).to_parquet(Path(tmp) / LISTS_FILE, index=False)
        (Path(tmp) / POPULARITY_FILE).write_text(json.dumps(pop), encoding="utf-8")
        mlflow.log_artifacts(tmp, ARTIFACT_DIR)


def redis_client() -> Any:
    import redis

    return redis.Redis.from_url(config.redis_url(), decode_responses=True)


def publish_candidates(client: MlflowClient, name: str, redis: Any = None) -> dict[str, Any]:
    version = champion_version(client, name)
    if version is None:
        raise RuntimeError(f"no champion for {name}: train and promote first")
    run_id = client.get_model_version(name, version).run_id
    local = Path(mlflow.artifacts.download_artifacts(run_id=run_id, artifact_path=ARTIFACT_DIR))
    lists = pd.read_parquet(local / LISTS_FILE)
    pop = json.loads((local / POPULARITY_FILE).read_text(encoding="utf-8"))
    counts = publish(redis if redis is not None else redis_client(), candidate_payloads(lists, pop))
    return {"model_version": version, **counts}
