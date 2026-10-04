"""Recommender: popularity baseline, stage-1 candidates (co-visitation + ALS), LightGBM rerank on a
time split of simulated sessions; MLflow runs, `recommender` registry, champion/challenger.

Stage 1 for the ranker's training rows is fit on sessions before `ranker_start` (so a training
session's own co-visits never leak into its features); stage 1 for test and serving is refit on
every session before `test_start`."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import lightgbm as lgb
import mlflow
import numpy as np
import pandas as pd
import yaml
from mlflow import MlflowClient
from mlflow.models import infer_signature

from retail_ml.config import check_promotion_mode, gold_dir
from retail_ml.data import sha256_file
from retail_ml.late_delivery.promote import CHALLENGER, approve, champion_version
from retail_ml.late_delivery.train import git_sha
from retail_ml.recommender.candidates import (
    Popularity,
    candidate_lists,
    covisitation,
    fit_als,
    interactions,
    popularity,
    product_events,
    session_pools,
    top_covis_matrix,
)
from retail_ml.recommender.publish import log_candidates
from retail_ml.recommender.ranker import (
    CATEGORICAL,
    RecommenderModel,
    Tables,
    model_inputs,
    session_features,
    session_prefixes,
    window_counts,
)

DEFAULT_CONFIG = Path(__file__).resolve().parents[3] / "configs" / "recommender.yaml"
DAY = pd.Timedelta(hours=24)
SESSION_INPUT = ["session_id", "event_type", "event_ts", "product_id", "is_purchase_target"]
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RecommenderConfig:
    experiment: str
    registered_model: str
    sessions_path: Path
    products_path: Path
    order_items_path: Path
    ranker_start: pd.Timestamp
    test_start: pd.Timestamp
    k: int
    n_candidates: int
    last_products: int
    pool_size: int
    pop_pool: int
    als: dict[str, Any]
    lightgbm: dict[str, Any]


def load_recommender_config(path: Path | None = None) -> RecommenderConfig:
    raw = yaml.safe_load((path or DEFAULT_CONFIG).read_text(encoding="utf-8"))
    return RecommenderConfig(
        experiment=raw["experiment"],
        registered_model=raw["registered_model"],
        sessions_path=gold_dir() / raw["sessions_path"],
        products_path=gold_dir() / raw["products_path"],
        order_items_path=gold_dir() / raw["order_items_path"],
        ranker_start=pd.Timestamp(raw["split"]["ranker_start"], tz="UTC"),
        test_start=pd.Timestamp(raw["split"]["test_start"], tz="UTC"),
        k=int(raw["k"]),
        n_candidates=int(raw["n_candidates"]),
        last_products=int(raw["last_products"]),
        pool_size=int(raw["pool_size"]),
        pop_pool=int(raw["pop_pool"]),
        als=dict(raw["als"]),
        lightgbm=dict(raw["lightgbm"]),
    )


def read_sessions(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path, columns=SESSION_INPUT)
    df = df[df["session_id"].notna()].reset_index(drop=True)
    df["event_ts"] = pd.to_datetime(df["event_ts"], utc=True)
    df["is_purchase_target"] = df["is_purchase_target"].fillna(False).astype(bool)
    return df


def read_catalogue(products_path: Path, order_items_path: Path) -> pd.DataFrame:
    """Current products (index product_id): `product_category_name` (the simulator's catalogue
    vocabulary; English maps missing categories to one placeholder) and median item price."""
    cols = ["product_id", "product_category_name", "is_current"]
    p = pd.read_parquet(products_path, columns=cols)
    p = p[p["is_current"].astype(bool)].drop_duplicates("product_id", keep="last")
    category = p["product_category_name"]
    items = pd.read_parquet(order_items_path, columns=["product_id", "price"])
    price = items["price"].astype("float64").groupby(items["product_id"]).median()
    out = pd.DataFrame({"category": category.to_numpy()}, index=pd.Index(p["product_id"]))
    return out.assign(price=price)


@dataclass
class Stage1:
    tables: Tables
    lists: pd.DataFrame
    pop: Popularity


def fit_stage1(events: pd.DataFrame, catalogue: pd.DataFrame, cfg: RecommenderConfig) -> Stage1:
    items = pd.Index(sorted(set(catalogue.index) | set(events["product_id"].dropna())))
    covis = covisitation(events, items)
    factors = fit_als(interactions(events, items), cfg.als)
    pop = popularity(events, catalogue["category"], cfg.n_candidates)
    lists = candidate_lists(covis, factors, items, catalogue["category"], pop, cfg.n_candidates)
    views = events.loc[events["event_type"] == "product_view", "product_id"].value_counts()
    covis_top = top_covis_matrix(covis, cfg.n_candidates)
    return Stage1(Tables.build(items, catalogue, views, covis_top, factors), lists, pop)


def rank_frame(
    events: pd.DataFrame,
    prefix: pd.DataFrame,
    targets: pd.DataFrame,
    feats: pd.DataFrame,
    stage: Stage1,
    cfg: RecommenderConfig,
) -> pd.DataFrame:
    """(session × pooled candidate) rows: model inputs, `cand_score`, `label` (purchase target).
    Candidate popularity is counted in the 24 dataset hours up to the prefix's last event."""
    split = feats["last_product_ids"].str.split(",")
    anchors = split.explode().replace("", None).rename("product_id").reset_index()
    viewed = product_events(prefix[prefix["session_id"].isin(feats.index)])
    pools = session_pools(
        anchors, viewed, stage.lists, stage.pop, feats["last_category"], cfg.pool_size, cfg.pop_pool
    )
    frame = pools.join(feats, on="session_id")
    for col, kind in (("views_24h", "product_view"), ("carts_24h", "add_to_cart")):
        frame[col] = window_counts(events, kind, frame["product_id"], frame["last_ts"], DAY)
    is_target = pd.MultiIndex.from_frame(frame[["session_id", "product_id"]]).isin(
        pd.MultiIndex.from_frame(targets[["session_id", "product_id"]])
    )
    return frame.assign(label=is_target.astype(int))


def fit_ranker(frame: pd.DataFrame, tables: Tables, params: dict[str, Any]) -> Any:
    """LambdaRank over sessions whose pool holds a purchase target (others carry no signal)."""
    frame = frame[frame.groupby("session_id")["label"].transform("max") > 0]
    X = tables.features(model_inputs(frame))
    group = frame.groupby("session_id", sort=False).size().to_numpy()
    return lgb.LGBMRanker(**params).fit(
        X, frame["label"].to_numpy(), group=group, categorical_feature=CATEGORICAL
    )


def popularity_ranking(
    feats: pd.DataFrame, viewed: pd.DataFrame, pop: Popularity, k: int
) -> pd.DataFrame:
    """Baseline: most viewed in the train window, the session's `last_category` first, then
    global; already-viewed products excluded."""
    seen = viewed.groupby("session_id")["product_id"].agg(set)
    rows = []
    for sid, cat in feats["last_category"].items():
        ranked = [*pop["category"].get(cat, []), *pop["global"]] if pd.notna(cat) else pop["global"]
        skip = seen.get(sid, set())
        picks = [p for p in dict.fromkeys(ranked) if p not in skip][:k]
        rows += [(sid, p, float(k - i)) for i, p in enumerate(picks)]
    return pd.DataFrame(rows, columns=["session_id", "product_id", "score"])


def ranking_metrics(
    ranked: pd.DataFrame, targets: pd.DataFrame, k: int, catalogue_size: int
) -> dict[str, float]:
    """Mean Recall@k and NDCG@k (binary relevance) over the sessions in `targets`, and the share
    of the catalogue appearing in any top-k. Ties keep the input order."""
    top = ranked.sort_values(["session_id", "score"], ascending=[True, False], kind="stable")
    top = top.assign(rank=top.groupby("session_id").cumcount() + 1)
    top = top[top["rank"] <= k]
    hits = top.merge(targets, on=["session_id", "product_id"])
    n_targets = targets.groupby("session_id").size()
    n_hits = hits.groupby("session_id").size().reindex(n_targets.index, fill_value=0)
    gain = 1 / np.log2(hits["rank"].to_numpy() + 1)
    dcg = pd.Series(gain).groupby(hits["session_id"].to_numpy()).sum()
    dcg = dcg.reindex(n_targets.index, fill_value=0.0)
    ideal = np.cumsum(1 / np.log2(np.arange(2, k + 2)))
    idcg = n_targets.clip(upper=k).map(lambda m: ideal[m - 1])
    return {
        f"recall_at_{k}": float((n_hits / n_targets).mean()),
        f"ndcg_at_{k}": float((dcg / idcg).mean()),
        f"coverage_at_{k}": top["product_id"].nunique() / catalogue_size,
    }


@dataclass(frozen=True)
class RecommenderDecision:
    promoted: bool
    reason: str
    challenger_ndcg: float
    champion_ndcg: float | None


def _scores(name: str, version: str, X: pd.DataFrame) -> np.ndarray:
    model = mlflow.pyfunc.load_model(f"models:/{name}/{version}")
    return np.asarray(model.predict(X), dtype="float64")


def promote_recommender(
    client: MlflowClient,
    name: str,
    version: str,
    frame: pd.DataFrame,
    targets: pd.DataFrame,
    k: int,
    catalogue_size: int,
    baseline_coverage: float,
    mode: str = "auto",
) -> RecommenderDecision:
    """Set `challenger`; in auto mode move `champion` when the model tests pass (no NaN scores,
    coverage@k >= the popularity baseline's) and test NDCG@k beats the champion's, re-scored on
    the same test rows."""
    mode = check_promotion_mode(mode)
    client.set_registered_model_alias(name, CHALLENGER, version)
    X = model_inputs(frame)
    keys = frame[["session_id", "product_id"]]

    def evaluate(p: np.ndarray) -> dict[str, float]:
        return ranking_metrics(keys.assign(score=p), targets, k, catalogue_size)

    p = _scores(name, version, X)
    if np.isnan(p).any():
        return RecommenderDecision(False, "model tests failed: nan scores", float("nan"), None)
    m = evaluate(p)
    score, coverage = m[f"ndcg_at_{k}"], m[f"coverage_at_{k}"]
    if coverage < baseline_coverage:
        reason = f"model tests failed: coverage {coverage:.4f} < baseline {baseline_coverage:.4f}"
        return RecommenderDecision(False, reason, score, None)

    champion = champion_version(client, name)
    champion_score = None
    reason = "no champion"
    if champion is not None:
        try:
            p_champion = _scores(name, champion, X)
            if not np.isnan(p_champion).any():
                champion_score = evaluate(p_champion)[f"ndcg_at_{k}"]
                reason = f"beats champion v{champion}"
        except Exception:
            logger.exception("champion v%s of %s could not be scored", champion, name)
        if champion_score is None:
            reason = "champion could not be scored"

    if champion_score is not None and not score > champion_score:
        reason = f"ndcg@{k} {score:.4f} <= champion v{champion} {champion_score:.4f}"
        return RecommenderDecision(False, reason, score, champion_score)
    if mode == "manual":
        print(f"approve with: retail-ml promote {name} --version {version}")
        return RecommenderDecision(
            False, f"manual mode: awaiting approval ({reason})", score, champion_score
        )
    approve(client, name, version)
    return RecommenderDecision(True, reason, score, champion_score)


@dataclass(frozen=True)
class RecommenderTrainResult:
    run_ids: dict[str, str]
    metrics: dict[str, dict[str, float]]
    version: str
    decision: RecommenderDecision


def train(cfg: RecommenderConfig, promotion_mode: str = "auto") -> RecommenderTrainResult:
    events = read_sessions(cfg.sessions_path)
    catalogue = read_catalogue(cfg.products_path, cfg.order_items_path)
    start = events.groupby("session_id")["event_ts"].min()
    started = events["session_id"].map(start)
    prefix, targets = session_prefixes(events)
    feats = session_features(prefix, catalogue["category"], cfg.last_products)
    first = start.reindex(feats.index)
    rank_feats = feats[(first >= cfg.ranker_start) & (first < cfg.test_start)]
    test_feats = feats[first >= cfg.test_start]
    test_targets = targets[targets["session_id"].isin(test_feats.index)]

    stage_a = fit_stage1(events[started < cfg.ranker_start], catalogue, cfg)
    fit_frame = rank_frame(events, prefix, targets, rank_feats, stage_a, cfg)
    ranker = fit_ranker(fit_frame, stage_a.tables, cfg.lightgbm)
    del stage_a, fit_frame  # one stage in memory at a time (2 GB ML containers)
    stage_b = fit_stage1(events[started < cfg.test_start], catalogue, cfg)
    test_frame = rank_frame(events, prefix, targets, test_feats, stage_b, cfg)
    model = RecommenderModel(stage_b.tables, ranker, cfg.pool_size, cfg.pop_pool)

    keys = test_frame[["session_id", "product_id"]]
    viewed = product_events(prefix[prefix["session_id"].isin(test_feats.index)])
    ranked = {
        "popularity": popularity_ranking(test_feats, viewed, stage_b.pop, cfg.k),
        "candidates": keys.assign(score=test_frame["cand_score"].to_numpy()),
        "lightgbm": keys.assign(score=model.predict(None, model_inputs(test_frame))),
    }
    size = len(catalogue)
    lineage = {
        "data_sha256": sha256_file(cfg.sessions_path),
        "catalogue_sha256": sha256_file(cfg.products_path),
        "git_sha": git_sha(),
    }
    common = {
        "ranker_start": cfg.ranker_start.date().isoformat(),
        "test_start": cfg.test_start.date().isoformat(),
        "k": cfg.k,
        "n_candidates": cfg.n_candidates,
        "pool_size": cfg.pool_size,
        "pop_pool": cfg.pop_pool,
        "last_products": cfg.last_products,
        "catalogue_size": size,
        "n_sessions_ranker": len(rank_feats),
        "n_sessions_test": len(test_feats),
    }
    params: dict[str, dict[str, Any]] = {
        "popularity": {},
        "candidates": {f"als_{k}": v for k, v in cfg.als.items()},
        "lightgbm": cfg.lightgbm,
    }

    mlflow.set_experiment(cfg.experiment)
    run_ids: dict[str, str] = {}
    metrics: dict[str, dict[str, float]] = {}
    model_uri = ""
    for model_type, frame in ranked.items():  # baseline first
        scores = {
            f"test_{m}": v for m, v in ranking_metrics(frame, test_targets, cfg.k, size).items()
        }
        if model_type == "candidates":
            whole = cfg.pool_size + cfg.pop_pool
            pool = ranking_metrics(frame, test_targets, whole, size)
            scores["test_pool_recall"] = pool[f"recall_at_{whole}"]
        with mlflow.start_run(run_name=model_type) as run:
            mlflow.set_tags({"model_type": model_type, **lineage})
            mlflow.log_params({**common, **params[model_type]})
            mlflow.log_metrics(scores)
            if model_type == "lightgbm":
                log_candidates(stage_b.lists, stage_b.pop)
                example = model_inputs(test_frame.head(5))
                info = mlflow.pyfunc.log_model(
                    name="model",
                    python_model=model,
                    signature=infer_signature(example, model.predict(None, example)),
                    input_example=example,
                )
                model_uri = info.model_uri
        run_ids[model_type], metrics[model_type] = run.info.run_id, scores

    del events, prefix, started, stage_b, model, ranked, keys  # promotion reloads from registry
    version = str(mlflow.register_model(model_uri, cfg.registered_model).version)
    decision = promote_recommender(
        MlflowClient(),
        cfg.registered_model,
        version,
        test_frame,
        test_targets,
        cfg.k,
        size,
        baseline_coverage=metrics["popularity"][f"test_coverage_at_{cfg.k}"],
        mode=promotion_mode,
    )
    return RecommenderTrainResult(run_ids, metrics, version, decision)
