"""`POST /recommend`: Feast `session_features` → Redis candidate lists of the session's last
products → the training pool (`session_pool` ≡ `candidates.session_pools`, sizes from the
champion) → one batch rerank by the `recommender` champion. Fallbacks (Review Focus 2/3): no
champion or no session products → popularity of the session's `last_category`, then global;
never an empty list. Online the viewed set is `last_product_ids` (≤ 5): offline pools also
exclude products viewed earlier in the session, which the feature view does not carry."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Literal, Protocol

import numpy as np
import pandas as pd
from feast import FeatureStore
from pydantic import BaseModel, ConfigDict, Field

from retail_ml import config
from retail_ml.data import feature_store
from retail_ml.recommender.candidates import Popularity
from retail_ml.recommender.ranker import INPUT_COLUMNS, POPULARITY_COLUMNS, SESSION_COLUMNS
from retail_ml.serving.model import FeatureStoreUnavailable, Predictor, load_champion

logger = logging.getLogger(__name__)
RECOMMENDER = "recommender"
SESSION_FIELDS = [*SESSION_COLUMNS, "last_category", "last_product_ids"]
SESSION_REFS = [f"session_features:{c}" for c in SESSION_FIELDS]
POPULARITY_REFS = [f"product_popularity:{c}" for c in POPULARITY_COLUMNS]
Strategy = Literal["rerank", "category_popularity", "global_popularity"]
Catalogue = dict[str, tuple[str | None, str | None]]  # product_id -> (category, category_en)


class RecommendRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    session_id: str = Field(min_length=1)
    k: int = Field(default=10, ge=1, le=50)


class Item(BaseModel):
    product_id: str
    score: float
    category: str | None  # product_category_name (Portuguese, the pop:category: vocabulary)
    category_en: str | None = None
    title: None = None  # Phase 6 enrichment


class RecommendResponse(BaseModel):
    session_id: str
    items: list[Item]
    model_version: str | None  # None for the popularity fallbacks
    strategy: Strategy


class OnlineFeatures(Protocol):
    def session(self, session_id: str) -> dict[str, Any] | None: ...

    def popularity(self, product_ids: list[str]) -> pd.DataFrame: ...


class CandidatesUnpublished(RuntimeError):
    pass


@dataclass(frozen=True)
class Recommender:
    version: str
    model: Predictor
    pool_size: int
    pop_pool: int


@dataclass(frozen=True)
class Sources:
    online: OnlineFeatures | None  # None: Feast registry missing -> every session is unknown
    redis: Any  # redis-py client (`mget`)
    catalogue: Catalogue


class FeastOnline:
    """Feast online reads. Feast 0.66 applies view TTLs only on its precomputed feature-service
    path, so rows stamped with 2017-2018 dataset time are returned (tested)."""

    def __init__(self, store: FeatureStore) -> None:
        self.store = store

    def session(self, session_id: str) -> dict[str, Any] | None:
        row = self.store.get_online_features(
            features=SESSION_REFS, entity_rows=[{"session_id": session_id}]
        ).to_dict()
        values = {c: row[c][0] for c in SESSION_FIELDS}
        return None if values["n_events"] is None else values

    def popularity(self, product_ids: list[str]) -> pd.DataFrame:
        rows = self.store.get_online_features(
            features=POPULARITY_REFS, entity_rows=[{"product_id": p} for p in product_ids]
        ).to_dict()
        frame = pd.DataFrame({c: rows[c] for c in POPULARITY_COLUMNS}, dtype="float64")
        return frame.fillna(0.0).set_index(pd.Index(product_ids, name="product_id"))


def load_recommender(name: str = RECOMMENDER) -> Recommender | None:
    loaded = load_champion(name)
    if loaded is None:
        return None
    python_model = loaded.model.unwrap_python_model()  # type: ignore[attr-defined]
    return Recommender(
        loaded.version, loaded.model, int(python_model.pool_size), int(python_model.pop_pool)
    )


def load_catalogue() -> Catalogue:
    """Current products' categories from gold `dim_product` (response metadata only)."""
    from retail_ml.recommender.train import load_recommender_config

    cols = ["product_id", "product_category_name", "product_category_name_english", "is_current"]
    try:
        p = pd.read_parquet(load_recommender_config().products_path, columns=cols)
    except Exception:
        logger.exception("product catalogue unreadable; items get category null")
        return {}
    p = p[p["is_current"].astype(bool)].drop_duplicates("product_id", keep="last")

    def name(v: Any) -> str | None:
        return v if isinstance(v, str) else None

    return {
        pid: (name(pt), name(en))
        for pid, pt, en in zip(
            p["product_id"],
            p["product_category_name"],
            p["product_category_name_english"],
            strict=True,
        )
    }


def load_sources() -> Sources:
    from retail_ml.recommender.publish import redis_client

    online: FeastOnline | None = None
    # Feast creates (writes) a registry it can't find; serving mounts it read-only
    if config.feast_registry_path().is_file():
        online = FeastOnline(feature_store())
    else:
        logger.warning("%s", FeatureStoreUnavailable("Feast registry not found"))
    return Sources(online, redis_client(), load_catalogue())


def _json(raw: str | None) -> Any:
    return json.loads(raw) if raw else None


def _popular(ranked: list[str], skip: set[str], k: int) -> list[str]:
    return [p for p in dict.fromkeys(ranked) if p not in skip][:k]


def session_pool(
    anchors: list[str],
    lists: dict[str, list[tuple[str, float]]],
    pop: Popularity,
    category: str | None,
    size: int,
    pop_size: int,
) -> list[str]:
    """`candidates.session_pools` for one session whose viewed products are its (distinct)
    anchors, in plain Python: the pandas version costs ~20 ms of a ~38 ms request (measured).
    A randomised test pins it to `session_pools` (order and tie-breaks included)."""
    viewed = set(anchors)
    # candidate -> [score sum, Kahan compensation, best rank]; Kahan like pandas' groupby sum,
    # so equal sums tie exactly as offline (a naive sum differs in the last bit)
    agg: dict[str, list[float]] = {}
    for a in anchors:
        for rank, (c, s) in enumerate(lists.get(a, [])):
            acc = agg.setdefault(c, [0.0, 0.0, rank])
            y = s - acc[1]
            t = acc[0] + y
            acc[1] = t - acc[0] - y
            acc[0] = t
            acc[2] = min(acc[2], rank)
    pooled = sorted((c for c in agg if c not in viewed), key=lambda c: (-agg[c][0], agg[c][2], c))
    pooled = pooled[:size]
    if category in pop["category"]:
        ranked = [*pop["category"][category], *pop["global"]]
    else:
        ranked = pop["global"]
    head = list(dict.fromkeys(ranked))[: 2 * pop_size]
    popular = [p for p in head if p not in viewed][:pop_size]
    have = set(pooled)
    return pooled + [p for p in popular if p not in have]


def _number(value: Any) -> float:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if np.isnan(f) else f


def rank_inputs(ids: list[str], row: dict[str, Any], popularity: pd.DataFrame) -> pd.DataFrame:
    """`ranker.model_inputs` of the (session, candidate) rows, built directly (tested equal)."""
    n = len(ids)
    data: dict[str, Any] = {
        "last_product_ids": np.array([str(row.get("last_product_ids") or "")] * n, dtype=object),
        **{c: np.full(n, _number(row.get(c))) for c in SESSION_COLUMNS},
        "product_id": np.array(ids, dtype=object),
    }
    for c in POPULARITY_COLUMNS:
        data[c] = np.nan_to_num(popularity[c].reindex(ids).to_numpy(dtype="float64"))
    return pd.DataFrame(data, columns=INPUT_COLUMNS)


def _rerank(
    row: dict[str, Any],
    anchors: list[str],
    lists: dict[str, list[tuple[str, float]]],
    pop: Popularity,
    category: str | None,
    model: Recommender,
    online: OnlineFeatures | None,
) -> tuple[list[str], np.ndarray]:
    """The training pool (anchors = the products the session is known to have viewed), scored
    in one batch. Returns ids and scores, best first."""
    ids = session_pool(anchors, lists, pop, category, model.pool_size, model.pop_pool)
    if not ids:
        return [], np.empty(0)
    popularity = pd.DataFrame(0.0, index=pd.Index(ids), columns=POPULARITY_COLUMNS)
    if online is not None:
        try:
            popularity = online.popularity(ids)
        except Exception:
            logger.exception("product popularity read failed; scored as 0")
    scores = np.asarray(model.model.predict(rank_inputs(ids, row, popularity)), dtype="float64")
    order = np.argsort(-scores, kind="stable")
    return [ids[i] for i in order], scores[order]


def recommend(
    req: RecommendRequest, sources: Sources, model: Recommender | None
) -> RecommendResponse:
    """Redis errors propagate (the caller answers 503); Feast read errors degrade to unknown."""
    sid, k = req.session_id, req.k
    row: dict[str, Any] | None = None
    if sources.online is not None:
        try:
            row = sources.online.session(sid)
        except Exception:
            logger.exception("session feature read failed; treated as an unknown session")
    row = row or {}
    anchors = list(dict.fromkeys(p for p in (row.get("last_product_ids") or "").split(",") if p))
    category = row.get("last_category") or None
    rerank = model is not None and bool(anchors)
    keys = ["pop:global", f"pop:category:{category}"]
    keys += [f"cand:{a}" for a in anchors] if rerank else []
    values = sources.redis.mget(keys)  # one round trip
    pop_global: list[str] = _json(values[0]) or []
    pop_category: list[str] = (_json(values[1]) if category else None) or []
    pop: Popularity = {"global": pop_global, "category": {}}
    if pop_category:
        pop["category"][category] = pop_category

    ids: list[str] = []
    scores: list[float] = []
    if rerank:
        assert model is not None
        lists = {
            a: [(c, float(s)) for c, s, _ in _json(raw) or []]
            for a, raw in zip(anchors, values[2:], strict=True)
        }
        ranked, raw_scores = _rerank(row, anchors, lists, pop, category, model, sources.online)
        ids, scores = ranked[:k], raw_scores[:k].tolist()
        strategy: Strategy = "rerank"
    else:
        strategy = "category_popularity" if pop_category else "global_popularity"
    if len(ids) < k:  # fallback, or a pool shorter than k: popularity below the reranked items
        fill = _popular([*pop_category, *pop_global], {*anchors, *ids}, k - len(ids))
        if ids:
            scores += [min(scores) - (i + 1) for i in range(len(fill))]
        else:
            scores = [1 / (i + 1) for i in range(len(fill))]
        ids += fill
    if not ids:
        raise CandidatesUnpublished(
            "no popularity lists in Redis; run `retail-ml publish-candidates`"
        )
    items = [
        Item(
            product_id=p,
            score=float(s),
            category=sources.catalogue.get(p, (None, None))[0],
            category_en=sources.catalogue.get(p, (None, None))[1],
        )
        for p, s in zip(ids, scores, strict=True)
    ]
    version = model.version if strategy == "rerank" and model is not None else None
    return RecommendResponse(session_id=sid, items=items, model_version=version, strategy=strategy)
