import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import mlflow
import numpy as np
import pandas as pd
import pytest
from mlflow import MlflowClient
from test_recommender import COLUMNS, FakeRedis

from retail_ml.cli import main
from retail_ml.data import sha256_file
from retail_ml.recommender import publish as publish_module
from retail_ml.recommender.ranker import model_inputs
from retail_ml.recommender.train import load_recommender_config, promote_recommender

CATEGORIES = ["brinquedos", "esporte_lazer", "cama_mesa_banho", "beleza_saude", "ferramentas"]
PER_CATEGORY = 60
NEW_PRODUCT = "p_new"  # in the catalogue, never in a session


def products() -> dict[str, list[str]]:
    return {c: [f"p{j:02d}{i:03d}" for i in range(PER_CATEGORY)] for j, c in enumerate(CATEGORIES)}


def synthetic_sessions(gold: Path, n_converting: int = 2400, n_browsing: int = 1200) -> None:
    """Offline-sessions contract with a planted preference: whoever views product i of a category
    buys product i+1 of that category; popularity is flat, so only stage 1 can find it."""
    rng = np.random.default_rng(11)
    by_cat = products()
    t0, days = pd.Timestamp("2017-10-01"), 334
    rows: list[dict[str, Any]] = []

    def add(sid: str, kind: str, at: pd.Timestamp, product: str | None, target: bool) -> None:
        rows.append(
            {
                "event_id": f"{sid}-{len(rows)}",
                "event_type": kind,
                "session_id": sid,
                "customer_id": None,
                "device": "mobile",
                "referrer": "google",
                "event_ts": at.strftime("%Y-%m-%dT%H:%M:%S"),
                "product_id": product,
                "search_query": None,
                "quantity": 1 if kind == "add_to_cart" else None,
                "order_id": f"o-{sid}" if kind == "checkout_started" else None,
                "is_purchase_target": target,
            }
        )

    for s in range(n_converting + n_browsing):
        sid = f"s{s:05d}"
        cat = by_cat[CATEGORIES[rng.integers(len(CATEGORIES))]]
        at = t0 + pd.Timedelta(seconds=int(rng.integers(days * 86400)))
        add(sid, "page_view", at, None, False)
        if s < n_converting:
            i = int(rng.integers(PER_CATEGORY))
            noise, anchor, bought = cat[int(rng.integers(PER_CATEGORY))], cat[i], cat[(i + 1) % 60]
            steps = [("product_view", noise), ("product_view", anchor)]
            steps += [("product_view", bought), ("add_to_cart", bought), ("checkout_started", None)]
            for n, (kind, product) in enumerate(steps, 1):
                add(sid, kind, at + pd.Timedelta(seconds=30 * n), product, product == bought)
        else:
            for n, j in enumerate(rng.integers(PER_CATEGORY, size=3), 1):
                add(sid, "product_view", at + pd.Timedelta(seconds=30 * n), cat[int(j)], False)
    (gold / "ml").mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, columns=COLUMNS).to_parquet(gold / "ml" / "sessions_offline.parquet")

    ids = [p for c in CATEGORIES for p in by_cat[c]] + [NEW_PRODUCT]
    cats = [c for c in CATEGORIES for _ in range(PER_CATEGORY)] + ["brinquedos"]
    english = {"brinquedos": "toys", "esporte_lazer": "sports_leisure"}  # unused: pt vocabulary
    pd.DataFrame(
        {
            "product_id": ids,
            "product_category_name": cats,
            "product_category_name_english": [english.get(c) for c in cats],
            "is_current": True,
        }
    ).to_parquet(gold / "dim_product.parquet")
    pd.DataFrame(
        {"product_id": ids, "price": [Decimal(f"{rng.uniform(5, 500):.2f}") for _ in ids]}
    ).to_parquet(gold / "fct_order_items.parquet")


@pytest.fixture(scope="module")
def trained(tmp_path_factory: pytest.TempPathFactory) -> Any:
    tmp = tmp_path_factory.mktemp("recommender")
    mp = pytest.MonkeyPatch()
    gold = tmp / "gold"
    mp.setenv("GOLD_DIR", str(gold))
    mp.chdir(tmp)
    uri = f"sqlite:///{(tmp / 'mlflow.db').as_posix()}"
    mp.setenv("MLFLOW_TRACKING_URI", uri)
    mlflow.set_tracking_uri(uri)
    synthetic_sessions(gold)
    assert main(["train", "recommender"]) == 0
    yield {"gold": gold, "cfg": load_recommender_config()}
    mp.undo()


def _runs() -> list[Any]:
    client = MlflowClient()
    exp = client.get_experiment_by_name("recommender")
    assert exp is not None
    return list(client.search_runs([exp.experiment_id], order_by=["attributes.start_time ASC"]))


def test_config_paths_follow_gold_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GOLD_DIR", str(tmp_path))
    cfg = load_recommender_config()
    assert cfg.sessions_path == tmp_path / "ml" / "sessions_offline.parquet"
    assert cfg.experiment == cfg.registered_model == "recommender"
    assert cfg.test_start == pd.Timestamp("2018-06-01", tz="UTC")
    assert (cfg.k, cfg.n_candidates, cfg.last_products) == (10, 100, 5)


def test_baseline_then_candidates_then_reranker_with_lineage(trained: Any) -> None:
    runs = _runs()
    assert [r.data.tags["model_type"] for r in runs] == ["popularity", "candidates", "lightgbm"]
    data_hash = sha256_file(trained["gold"] / "ml" / "sessions_offline.parquet")
    for r in runs:
        assert r.data.tags["data_sha256"] == data_hash and r.data.tags["git_sha"]
        assert {"test_recall_at_10", "test_ndcg_at_10", "test_coverage_at_10"} <= set(
            r.data.metrics
        )


def test_reranker_beats_popularity_on_planted_preference(trained: Any) -> None:
    m = {r.data.tags["model_type"]: r.data.metrics for r in _runs()}
    base, cand, rerank = m["popularity"], m["candidates"], m["lightgbm"]
    print({k: {n: round(v, 4) for n, v in d.items()} for k, d in m.items()})
    # floor: flat popularity finds ~10/59 of a category; the planted next item is co-visited
    assert base["test_recall_at_10"] < 0.3
    assert rerank["test_recall_at_10"] >= base["test_recall_at_10"] + 0.4
    assert rerank["test_ndcg_at_10"] >= base["test_ndcg_at_10"] + 0.3
    assert rerank["test_ndcg_at_10"] >= cand["test_ndcg_at_10"] - 0.02
    assert rerank["test_coverage_at_10"] >= base["test_coverage_at_10"]


def test_first_model_promoted_and_identical_retrain_is_not(trained: Any, capsys: Any) -> None:
    client = MlflowClient()
    champion = client.get_model_version_by_alias("recommender", "champion")
    assert str(champion.version) == "1"
    capsys.readouterr()
    assert main(["train", "recommender"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["version"] == "2" and out["decision"]["promoted"] is False
    assert "<= champion v1" in out["decision"]["reason"]
    assert client.get_model_version_by_alias("recommender", "challenger").version == 2
    assert client.get_model_version_by_alias("recommender", "champion").version == 1


def test_artefact_scores_unknown_products_and_empty_sessions(trained: Any) -> None:
    model = mlflow.pyfunc.load_model("models:/recommender@champion")
    frame = pd.DataFrame(
        {
            "last_product_ids": ["p00000,p00001", "", "never_seen"],
            "n_events": [4, 1, 2],
            "n_product_views": [2, 0, 1],
            "n_categories": [1, 0, 0],
            "n_cart_adds": [0, 0, 0],
            "dwell_seconds": [60.0, 0.0, 5.0],
            "product_id": ["p00002", "p00003", "also_never_seen"],
            "views_24h": [1, 0, 0],
            "carts_24h": [0, 0, 0],
        }
    )
    scores = np.asarray(model.predict(model_inputs(frame)))
    assert scores.shape == (3,) and np.isfinite(scores).all()


def test_coverage_below_baseline_fails_the_model_tests(trained: Any) -> None:
    frame = pd.DataFrame(
        {
            "session_id": ["s1", "s1"],
            "last_product_ids": ["p00000", "p00000"],
            "n_events": [3.0, 3.0],
            "n_product_views": [1.0, 1.0],
            "n_categories": [1.0, 1.0],
            "n_cart_adds": [0.0, 0.0],
            "dwell_seconds": [30.0, 30.0],
            "product_id": ["p00001", "p00002"],
            "views_24h": [0.0, 0.0],
            "carts_24h": [0.0, 0.0],
        }
    )
    targets = pd.DataFrame({"session_id": ["s1"], "product_id": ["p00001"]})
    decision = promote_recommender(
        MlflowClient(), "recommender", "1", frame, targets, 10, 301, baseline_coverage=0.5
    )
    assert not decision.promoted and "coverage" in decision.reason
    assert MlflowClient().get_model_version_by_alias("recommender", "champion").version == 1


def test_publish_candidates_writes_champion_lists_to_redis(
    trained: Any, monkeypatch: pytest.MonkeyPatch, capsys: Any
) -> None:
    fake = FakeRedis()
    monkeypatch.setattr(publish_module, "redis_client", lambda: fake)
    capsys.readouterr()
    assert main(["publish-candidates"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["model_version"] == "1"
    cand = {k: json.loads(v) for k, v in fake.data.items() if k.startswith("cand:")}
    assert len(cand) == len(CATEGORIES) * PER_CATEGORY + 1 == out["candidate_keys"]
    assert all(0 < len(v) <= 100 for v in cand.values())
    first = cand["cand:p00000"][0]
    assert isinstance(first[0], str) and isinstance(first[1], float)
    assert first[2] in {"covis", "als", "covis+als", "popularity"}
    # Review Focus 3: the never-seen product gets its category's popularity
    assert {s for _, _, s in cand[f"cand:{NEW_PRODUCT}"]} == {"popularity"}
    toys = set(json.loads(fake.data["pop:category:brinquedos"]))
    assert {p for p, _, _ in cand[f"cand:{NEW_PRODUCT}"]} >= toys - {NEW_PRODUCT}
    assert "pop:category:toys" not in fake.data  # Portuguese vocabulary, as the simulator
    assert len(json.loads(fake.data["pop:global"])) == 100
