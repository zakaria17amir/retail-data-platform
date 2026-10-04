"""Stage 2: session prefixes, session/candidate features and the LightGBM reranker artefact.

The artefact takes the same columns `/recommend` gets online (Feast `session_features` +
`product_popularity` per candidate) and derives everything else from its own tables, so training
and serving share one feature function."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from mlflow.pyfunc.model import PythonModel
from scipy import sparse

from retail_ml.recommender.candidates import normalise, product_events

SESSION_COLUMNS = ["n_events", "n_product_views", "n_categories", "n_cart_adds", "dwell_seconds"]
POPULARITY_COLUMNS = ["views_24h", "carts_24h"]
INPUT_COLUMNS = ["last_product_ids", *SESSION_COLUMNS, "product_id", *POPULARITY_COLUMNS]
FEATURES = [
    *SESSION_COLUMNS,
    "last_category",
    "covis_score",
    "covis_max",
    "als_score",
    *POPULARITY_COLUMNS,
    "views_train",
    "same_category",
    "price_band",
]
CATEGORICAL = ["last_category"]


def session_prefixes(events: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Converting sessions cut before their last `product_view` preceding `checkout_started`
    (that view is the purchase in the simulator), and their purchase targets minus products
    already viewed in the prefix (`/recommend` never returns those)."""
    df = events.sort_values(["session_id", "event_ts"], kind="stable").reset_index(drop=True)
    pos = df.groupby("session_id").cumcount()
    checkout = pos.where(df["event_type"] == "checkout_started").groupby(df["session_id"])
    first_checkout = checkout.transform("min")
    view = pos.where((df["event_type"] == "product_view") & (pos < first_checkout))
    last_view = view.groupby(df["session_id"]).transform("max")
    prefix = df[pos < last_view].reset_index(drop=True)
    is_target = df["is_purchase_target"].astype(bool) & df["product_id"].notna()
    targets = df.loc[is_target & df["session_id"].isin(prefix["session_id"]), ["session_id"]]
    targets = targets.assign(product_id=df.loc[targets.index, "product_id"]).drop_duplicates()
    seen = (
        targets.merge(
            prefix[["session_id", "product_id"]].drop_duplicates(), how="left", indicator=True
        )["_merge"]
        .eq("both")
        .to_numpy()
    )
    return prefix, targets[~seen].reset_index(drop=True)


def session_features(prefix: pd.DataFrame, category: pd.Series, last_n: int) -> pd.DataFrame:
    """Per session (index session_id): the `session_features` view columns plus `last_ts`.
    `last_product_ids` = the last `last_n` distinct products, oldest first, comma-joined."""
    sid = prefix["session_id"]
    kind = prefix["event_type"]
    ts = prefix.groupby(sid)["event_ts"]
    prods = product_events(prefix)
    last = prods.drop_duplicates(["session_id", "product_id"], keep="last")
    last = last.sort_values(["session_id", "event_ts"], kind="stable")
    last = last[last.groupby("session_id").cumcount(ascending=False) < last_n]
    out = pd.DataFrame(
        {
            "n_events": sid.value_counts(),
            "n_product_views": kind.eq("product_view").groupby(sid).sum(),
            "n_categories": prods["product_id"]
            .map(category)
            .groupby(prods["session_id"])
            .nunique(),
            "n_cart_adds": kind.eq("add_to_cart").groupby(sid).sum(),
            "dwell_seconds": (ts.max() - ts.min()).dt.total_seconds(),
            "last_product_ids": last.groupby("session_id")["product_id"].agg(",".join),
            "last_ts": ts.max(),
        }
    )
    out[SESSION_COLUMNS] = out[SESSION_COLUMNS].fillna(0).astype("float64")
    out["last_product_ids"] = out["last_product_ids"].fillna("")
    out.index.name = "session_id"
    return out


def window_counts(
    events: pd.DataFrame,
    event_type: str,
    product_ids: pd.Series,
    at: pd.Series,
    window: pd.Timedelta,
) -> np.ndarray:
    """`event_type` events per (product, time) query in the dataset-time window (at-window, at]."""
    ev = events[(events["event_type"] == event_type) & events["product_id"].notna()]
    codes, _ = pd.factorize(pd.concat([ev["product_id"], product_ids], ignore_index=True))
    ev_code, q_code = codes[: len(ev)], codes[len(ev) :]
    seconds = pd.concat([ev["event_ts"], at], ignore_index=True).astype("int64") // 10**9
    w = int(window.total_seconds())
    t0 = int(seconds.min()) - w - 1
    span = int(seconds.max()) - t0 + 1
    ev_t, q_t = seconds.to_numpy()[: len(ev)] - t0, seconds.to_numpy()[len(ev) :] - t0
    keys = np.sort(ev_code.astype("int64") * span + ev_t)
    q = q_code.astype("int64") * span
    hi = np.searchsorted(keys, q + q_t, side="right")
    lo = np.searchsorted(keys, q + q_t - w, side="right")
    counts: np.ndarray = (hi - lo).astype("float64")
    return counts


def price_bands(price: pd.Series, bands: int = 5) -> pd.Series:
    """Catalogue price quantile band 0..bands-1 per product; -1 when the price is unknown."""
    if price.notna().sum() < bands:
        return pd.Series(-1, index=price.index, dtype="int8")
    band = pd.qcut(price.rank(method="first"), bands, labels=False)
    return band.fillna(-1).astype("int8")


@dataclass
class Tables:
    """Stage-1 lookups the ranker features need: product index, category codes, price bands,
    log views in the stage-1 window, scaled top-n co-visitation and normalised ALS factors."""

    items: pd.Index
    category: np.ndarray  # code per item, -1 unknown
    price_band: np.ndarray
    views: np.ndarray
    covis: sparse.csr_matrix
    factors: np.ndarray

    @classmethod
    def build(
        cls,
        items: pd.Index,
        catalogue: pd.DataFrame,
        views: pd.Series,
        covis_top: sparse.csr_matrix,
        factors: np.ndarray,
    ) -> Tables:
        cats = catalogue["category"].reindex(items)
        codes = pd.Categorical(cats, categories=sorted(cats.dropna().unique())).codes
        band = price_bands(catalogue["price"]).reindex(items).fillna(-1).astype("int8")
        log_views = np.log1p(views.reindex(items).fillna(0).to_numpy(dtype="float64"))
        return cls(
            items,
            codes.astype("int16"),
            band.to_numpy(),
            log_views.astype("float32"),
            covis_top,
            normalise(factors),
        )

    def features(self, frame: pd.DataFrame, chunk: int = 100_000) -> pd.DataFrame:
        """Per row: session counts, category of the session's last product, co-vis (sum and max
        over the session's last products) and ALS (max cosine) to the candidate, popularity,
        same-category flag, price band. Unknown products score 0 / code -1, never NaN.
        Rows are independent, so large frames are built `chunk` rows at a time (float32)."""
        n = len(frame)
        if n == 0:
            return pd.DataFrame(columns=FEATURES, dtype="float32")
        if n > chunk:
            parts = [self.features(frame.iloc[s : s + chunk], chunk) for s in range(0, n, chunk)]
            return pd.concat(parts, ignore_index=True)
        cand = self.items.get_indexer(pd.Index(frame["product_id"].astype(str)))
        groups, uniques = pd.factorize(frame["last_product_ids"].fillna("").astype(str))
        per = [self.items.get_indexer(pd.Index([p for p in u.split(",") if p])) for u in uniques]
        per = [a[a >= 0] for a in per]
        lens = np.array([len(a) for a in per], dtype="int64")
        last = np.array([a[-1] if len(a) else -1 for a in per], dtype="int64")[groups]
        flat = np.concatenate(per)
        rep = lens[groups]
        row = np.repeat(np.arange(n), rep)
        first = np.cumsum(rep) - rep
        anchor = flat[
            np.repeat((np.cumsum(lens) - lens)[groups], rep)
            + np.arange(len(row))
            - np.repeat(first, rep)
        ]
        covis, als = np.zeros(len(row)), np.zeros(len(row))
        c = cand[row]
        ok = c >= 0
        if ok.any():
            covis[ok] = np.asarray(self.covis[anchor[ok], c[ok]]).ravel()
            als[ok] = np.einsum("ij,ij->i", self.factors[anchor[ok]], self.factors[c[ok]])
        has = rep > 0
        covis_max, als_max = np.zeros(n), np.zeros(n)
        if has.any():
            covis_max[has] = np.maximum.reduceat(covis, first[has])
            als_max[has] = np.maximum.reduceat(als, first[has])
        last_cat = np.where(last >= 0, self.category[last], -1)
        cand_cat = np.where(cand >= 0, self.category[cand], -1)
        X = frame[SESSION_COLUMNS].astype("float64").reset_index(drop=True)
        X["last_category"] = last_cat.astype("int64")
        X["covis_score"] = np.bincount(row, covis, minlength=n)
        X["covis_max"] = covis_max
        X["als_score"] = als_max
        for col in POPULARITY_COLUMNS:
            X[col] = frame[col].astype("float64").to_numpy()
        X["same_category"] = ((last_cat >= 0) & (last_cat == cand_cat)).astype("float64")
        X["views_train"] = np.where(cand >= 0, self.views[cand], 0.0).astype("float64")
        X["price_band"] = np.where(cand >= 0, self.price_band[cand], -1).astype("float64")
        return X[FEATURES].astype("float32")


def model_inputs(frame: pd.DataFrame) -> pd.DataFrame:
    """Signature shape: string ids, float counts (what `/recommend` assembles per candidate)."""
    X = frame[INPUT_COLUMNS].copy()
    for col in ("last_product_ids", "product_id"):
        X[col] = X[col].fillna("").astype(str).astype("object")
    for col in [*SESSION_COLUMNS, *POPULARITY_COLUMNS]:
        X[col] = pd.to_numeric(X[col], errors="coerce").fillna(0).astype("float64")
    return X.reset_index(drop=True)


class RecommenderModel(PythonModel):
    """One row per (session, candidate) in `INPUT_COLUMNS`, one rerank score per row out."""

    def __init__(self, tables: Tables, ranker: Any, pool_size: int, pop_pool: int) -> None:
        self.tables, self.ranker = tables, ranker
        self.pool_size, self.pop_pool = pool_size, pop_pool  # for `session_pools` at serving

    def predict(self, context: Any, model_input: pd.DataFrame, params: Any = None) -> Any:
        X = self.tables.features(model_input.reset_index(drop=True))
        return np.asarray(self.ranker.predict(X), dtype="float64")
