"""Stage 1: item co-visitation + implicit ALS neighbours per product, popularity lists, and the
per-session candidate pool (union over the session's last products)."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from scipy import sparse

PRODUCT_EVENTS = ("product_view", "add_to_cart")
WEIGHTS = {"product_view": 1.0, "add_to_cart": 3.0}
PURCHASE_WEIGHT = 5.0
Popularity = dict[str, Any]  # {"global": [pid, ...], "category": {category: [pid, ...]}}


def product_events(events: pd.DataFrame) -> pd.DataFrame:
    """Product interactions in session order (event_ts; file order breaks ties)."""
    df = events[events["event_type"].isin(PRODUCT_EVENTS) & events["product_id"].notna()]
    return df.sort_values(["session_id", "event_ts"], kind="stable")


def covisitation(events: pd.DataFrame, items: pd.Index) -> sparse.csr_matrix:
    """Symmetric item × item weights: every pair of distinct products in a session adds
    1 / (distance between their positions in the session's product-event sequence)."""
    df = product_events(events)
    codes = items.get_indexer(pd.Index(df["product_id"]))
    session = pd.factorize(df["session_id"])[0]
    n = len(items)
    m = sparse.csr_matrix((n, n), dtype=np.float64)
    longest = int(np.bincount(session).max()) if len(session) else 0
    for d in range(1, longest):  # one sparse sum per distance keeps memory at the pair count
        pair = (session[:-d] == session[d:]) & (codes[:-d] != codes[d:])
        a, b = codes[:-d][pair], codes[d:][pair]
        m = m + sparse.csr_matrix((np.full(len(a), 1.0 / d), (a, b)), shape=(n, n))
    return (m + m.T).tocsr()


def interactions(events: pd.DataFrame, items: pd.Index) -> sparse.csr_matrix:
    """Session × item confidence: the strongest of view 1, cart 3, purchase 5."""
    df = events[events["product_id"].notna()]
    weight = df["event_type"].map(WEIGHTS).fillna(0.0)
    weight = weight.where(~df["is_purchase_target"].astype(bool), PURCHASE_WEIGHT)
    session = pd.factorize(df["session_id"])[0]
    item = items.get_indexer(pd.Index(df["product_id"]))
    pairs = pd.DataFrame({"s": session, "i": item, "w": weight})
    m = pairs[pairs["w"] > 0].groupby(["s", "i"], as_index=False)["w"].max()
    shape = (int(session.max()) + 1 if len(session) else 0, len(items))
    return sparse.csr_matrix((m["w"].to_numpy(), (m["s"], m["i"])), shape=shape)


def fit_als(matrix: sparse.csr_matrix, params: dict[str, Any]) -> np.ndarray:
    """`implicit` ALS item factors (items × factors); items without interactions get zeros."""
    from implicit.als import AlternatingLeastSquares
    from threadpoolctl import threadpool_limits

    with threadpool_limits(1, "blas"):  # implicit's own threads; nested BLAS threads slow it
        model = AlternatingLeastSquares(**params, use_gpu=False)
        model.fit(matrix.astype(np.float32).tocsr(), show_progress=False)
    factors = np.asarray(model.item_factors, dtype=np.float32)
    factors[np.asarray(matrix.getnnz(axis=0)) == 0] = 0.0
    return factors


def normalise(factors: np.ndarray) -> np.ndarray:
    norm = np.linalg.norm(factors, axis=1, keepdims=True)
    out: np.ndarray = np.divide(factors, norm, out=np.zeros_like(factors), where=norm > 0)
    return out


def _top_covis(covis: sparse.csr_matrix, n: int) -> pd.DataFrame:
    """Top-n co-visited items per anchor, scores scaled by the anchor's best (0, 1]."""
    anchors, cands, scores = [], [], []
    for a in range(covis.shape[0]):
        lo, hi = covis.indptr[a], covis.indptr[a + 1]
        if lo == hi:
            continue
        w, idx = covis.data[lo:hi], covis.indices[lo:hi]
        top = np.argsort(-w, kind="stable")[:n]
        anchors.append(np.full(len(top), a))
        cands.append(idx[top])
        scores.append(w[top] / w[top[0]])
    if not anchors:
        return pd.DataFrame({"a": [], "c": [], "covis": []})
    return pd.DataFrame(
        {"a": np.concatenate(anchors), "c": np.concatenate(cands), "covis": np.concatenate(scores)}
    )


def top_covis_matrix(covis: sparse.csr_matrix, n: int) -> sparse.csr_matrix:
    t = _top_covis(covis, n)
    coords = (t["a"].to_numpy(dtype=int), t["c"].to_numpy(dtype=int))
    return sparse.csr_matrix(
        (t["covis"].to_numpy(dtype=np.float32), coords), shape=covis.shape, dtype=np.float32
    )


def _top_als(factors: np.ndarray, n: int, chunk: int = 512) -> pd.DataFrame:
    """Top-n items by cosine similarity of ALS factors per anchor (zero vectors excluded)."""
    f = normalise(factors)
    live = np.flatnonzero(f.any(axis=1))
    frames = []
    for start in range(0, len(live), chunk):
        rows = live[start : start + chunk]
        sim = f[rows] @ f[live].T
        sim[np.arange(len(rows)), np.searchsorted(live, rows)] = -np.inf
        k = min(n, len(live) - 1)
        if k <= 0:
            break
        top = np.argpartition(-sim, k - 1, axis=1)[:, :k]
        s = np.take_along_axis(sim, top, axis=1)
        frames.append(
            pd.DataFrame({"a": np.repeat(rows, k), "c": live[top].ravel(), "als": s.ravel()})
        )
    if not frames:
        return pd.DataFrame({"a": [], "c": [], "als": []})
    return pd.concat(frames, ignore_index=True)


def popularity(events: pd.DataFrame, category: pd.Series, n: int) -> Popularity:
    """Most viewed products (ties by product_id), globally and per catalogue category."""
    views = events.loc[events["event_type"] == "product_view", "product_id"].dropna()
    counts = views.value_counts().rename("views").reset_index()
    counts = counts.sort_values(["views", "product_id"], ascending=[False, True])
    counts["category"] = counts["product_id"].map(category)
    by_category = {
        str(c): g["product_id"].head(n).tolist() for c, g in counts.groupby("category", sort=True)
    }
    return {"global": counts["product_id"].head(n).tolist(), "category": by_category}


def candidate_lists(
    covis: sparse.csr_matrix,
    factors: np.ndarray,
    items: pd.Index,
    category: pd.Series,
    pop: Popularity,
    n: int,
) -> pd.DataFrame:
    """Per anchor product: union of its top-n co-visitation and top-n ALS neighbours, score =
    scaled co-vis + ALS cosine, source tag, top-n by score; lists shorter than n (new or rare
    products) are padded with category then global popularity (score 0, source `popularity`).
    Columns anchor, candidate, score, source; ordered by anchor then rank."""
    merged = _top_covis(covis, n).merge(_top_als(factors, n), on=["a", "c"], how="outer")
    has_covis, has_als = merged["covis"].notna(), merged["als"].notna()
    merged["score"] = merged["covis"].fillna(0.0) + merged["als"].fillna(0.0)
    codes: Any = np.select([has_covis & has_als, has_covis], [0, 1], default=2)
    merged["source"] = pd.Categorical.from_codes(codes, pd.Index(["covis+als", "covis", "als"]))
    merged = merged.sort_values(["a", "score", "c"], ascending=[True, False, True], kind="stable")
    merged = merged[merged.groupby("a").cumcount() < n]
    ids = items.to_numpy()
    lists = pd.DataFrame(
        {
            "anchor": ids[merged["a"].to_numpy(dtype=int)],
            "candidate": ids[merged["c"].to_numpy(dtype=int)],
            "score": merged["score"].to_numpy(),
            "source": merged["source"].to_numpy(),
        }
    )
    held = lists.groupby("anchor")["candidate"].agg(list).to_dict()
    padding = []
    for anchor in items:
        have = held.get(anchor, [])
        if len(have) >= n:
            continue
        seen = {anchor, *have}
        cat = category.get(anchor)
        fill = [*pop["category"].get(cat, []), *pop["global"]] if pd.notna(cat) else pop["global"]
        extra = [p for p in dict.fromkeys(fill) if p not in seen][: n - len(have)]
        padding += [(anchor, p, 0.0, "popularity") for p in extra]
    pad = pd.DataFrame(padding, columns=lists.columns)
    out = pd.concat([lists, pad], ignore_index=True) if len(pad) else lists
    out["source"] = out["source"].astype("category")  # one string object per tag, not per row
    return out.sort_values("anchor", kind="stable").reset_index(drop=True)


def _unviewed(rows: pd.DataFrame, viewed: pd.DataFrame) -> pd.DataFrame:
    keys = viewed[["session_id", "product_id"]].drop_duplicates()
    seen = rows[["session_id", "product_id"]].merge(keys, how="left", indicator=True)["_merge"]
    return rows[~seen.eq("both").to_numpy()]


def session_pools(
    anchors: pd.DataFrame,
    viewed: pd.DataFrame,
    lists: pd.DataFrame,
    pop: Popularity,
    last_category: pd.Series,
    size: int,
    pop_size: int,
) -> pd.DataFrame:
    """Candidate pool per session (every session in `anchors`; product_id None = no anchor):
    the top `size` of its anchors' lists (score summed over anchors), then the top `pop_size`
    of its last category's popularity followed by global popularity (score 0), already-viewed
    products removed, so no session is ever empty. Columns session_id, product_id, cand_score;
    ordered by session then candidates-only rank."""
    hits = anchors.merge(
        lists[["anchor", "candidate", "score"]], left_on="product_id", right_on="anchor"
    )
    hits["rank"] = hits.groupby(["session_id", "anchor"]).cumcount()
    pooled = hits.groupby(["session_id", "candidate"], as_index=False).agg(
        cand_score=("score", "sum"), rank=("rank", "min")
    )
    pooled = _unviewed(pooled.rename(columns={"candidate": "product_id"}), viewed)
    pooled = pooled.sort_values(
        ["session_id", "cand_score", "rank"], ascending=[True, False, True], kind="stable"
    )
    pooled = pooled[pooled.groupby("session_id").cumcount() < size]

    ranked = {"": pop["global"]} | {c: [*ids, *pop["global"]] for c, ids in pop["category"].items()}
    long = pd.DataFrame(
        [
            (c, p, r)
            for c, ids in ranked.items()
            for r, p in enumerate(list(dict.fromkeys(ids))[: 2 * pop_size])
        ],
        columns=["category", "product_id", "rank"],
    )
    sessions = anchors["session_id"].drop_duplicates()
    cats = last_category.reindex(sessions).where(lambda c: c.isin(ranked), "").fillna("")
    popular = pd.DataFrame({"session_id": sessions.to_numpy(), "category": cats.to_numpy()})
    popular = _unviewed(popular.merge(long, on="category"), viewed)
    popular = popular[popular.groupby("session_id").cumcount() < pop_size]
    popular = popular.assign(cand_score=0.0, rank=popular["rank"] + 10**6)

    pool = pd.concat([pooled, popular[pooled.columns]], ignore_index=True)
    pool = pool.drop_duplicates(["session_id", "product_id"]).sort_values(
        "session_id", kind="stable"
    )
    return pool[["session_id", "product_id", "cand_score"]].reset_index(drop=True)
