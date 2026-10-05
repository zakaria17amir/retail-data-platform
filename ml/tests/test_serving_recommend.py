from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import lightgbm as lgb
import mlflow
import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
import redis
from conftest import local_store
from deltalake import write_deltalake
from fastapi.testclient import TestClient
from feast.data_source import PushMode
from mlflow import MlflowClient
from prometheus_client.parser import text_string_to_metric_families

from retail_ml.recommender.candidates import session_pools
from retail_ml.recommender.ranker import (
    INPUT_COLUMNS,
    POPULARITY_COLUMNS,
    SESSION_COLUMNS,
    RecommenderModel,
    model_inputs,
)
from retail_ml.serving.app import create_app
from retail_ml.serving.model import LoadedModel, single_threaded
from retail_ml.serving.recommend import (
    FeastOnline,
    Recommender,
    Sources,
    load_recommender,
    load_titles,
    rank_inputs,
    session_pool,
)

TS_UTC = pa.timestamp("us", tz="UTC")  # genai.enrichment.writer.ENRICHED_SCHEMA

SESSION = {
    "n_events": 6,
    "n_product_views": 4,
    "n_categories": 2,
    "last_category": "brinquedos",
    "last_product_ids": "p2,p1",  # streaming order: most recent first
    "n_cart_adds": 1,
    "dwell_seconds": 120.0,
}
CANDIDATES = {
    "p2": [["a", 0.9, "covis"], ["b", 0.5, "als"], ["p1", 0.4, "covis"]],
    "p1": [["b", 0.7, "covis"], ["c", 0.2, "als"]],
}
POP = {
    "pop:global": ["g1", "g2", "p1", "g3", "g4", "g5", "g6", "g7", "g8", "g9", "g10", "g11"],
    "pop:category:brinquedos": ["t1", "p2", "t2", "t3"],
}
CATALOGUE = {
    "a": ("brinquedos", "toys"),
    "b": ("bebes", "baby"),
    "t1": ("brinquedos", "toys"),
}


class FakeRedis:
    def __init__(self, values: dict[str, Any], down: bool = False) -> None:
        self.values = {k: json.dumps(v) for k, v in values.items()}
        self.down = down
        self.calls: list[list[str]] = []

    def mget(self, keys: list[str]) -> list[str | None]:
        if self.down:
            raise redis.exceptions.ConnectionError("redis down")
        self.calls.append(list(keys))
        return [self.values.get(k) for k in keys]


class FakeOnline:
    def __init__(self, sessions: dict[str, dict[str, Any]], broken: bool = False) -> None:
        self.sessions = sessions
        self.broken = broken

    def session(self, session_id: str) -> dict[str, Any] | None:
        if self.broken:
            raise RuntimeError("feast registry unreadable")
        return self.sessions.get(session_id)

    def popularity(self, product_ids: list[str]) -> pd.DataFrame:
        views = {"b": 30.0, "c": 5.0}
        return pd.DataFrame(
            {"views_24h": [views.get(p, 0.0) for p in product_ids], "carts_24h": 1.0},
            index=pd.Index(product_ids, name="product_id"),
        )


class StubRanker:
    """Scores = rank of the product in `prefer` (unknown products last)."""

    def __init__(self, prefer: list[str]) -> None:
        self.prefer = prefer
        self.inputs: list[pd.DataFrame] = []

    def predict(self, df: pd.DataFrame) -> Any:
        self.inputs.append(df)
        order = {p: len(self.prefer) - i for i, p in enumerate(self.prefer)}
        return np.array([float(order.get(p, -1)) for p in df["product_id"]])


def _client(
    *,
    recommender: Recommender | None,
    redis_values: dict[str, Any] | None = None,
    online: Any = None,
    down: bool = False,
    titles: dict[str, str] | None = None,
) -> TestClient:
    values = {**POP, **{f"cand:{k}": v for k, v in CANDIDATES.items()}}
    sources = Sources(
        online=online if online is not None else FakeOnline({"s1": SESSION}),
        redis=FakeRedis(values if redis_values is None else redis_values, down=down),
        catalogue=CATALOGUE,
        titles=titles or {},
    )
    app = create_app(
        load_model=lambda: None,
        load_lookup=lambda: lambda seller_id: {},
        load_recommender=lambda: recommender,
        load_sources=lambda: sources,
    )
    return TestClient(app)


@pytest.fixture
def ranker() -> StubRanker:
    return StubRanker(prefer=["c", "t2", "b", "a"])


@pytest.fixture
def client(ranker: StubRanker) -> Iterator[TestClient]:
    with _client(recommender=Recommender("4", ranker, pool_size=3, pop_pool=2)) as c:
        yield c


def test_rerank_scores_the_offline_pool_in_one_batch(
    client: TestClient, ranker: StubRanker
) -> None:
    r = client.post("/recommend", json={"session_id": "s1", "k": 3})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["strategy"] == "rerank" and body["model_version"] == "4"
    assert [i["product_id"] for i in body["items"]] == ["c", "t2", "b"]
    assert body["items"][2] == {
        "product_id": "b",
        "score": 2.0,
        "category": "bebes",
        "category_en": "baby",
        "title": None,
    }
    assert body["items"][1]["category"] is None  # not in the catalogue
    (X,) = ranker.inputs  # one batch call
    assert list(X.columns) == INPUT_COLUMNS
    anchors = pd.DataFrame({"session_id": "s1", "product_id": ["p2", "p1"]})
    lists = pd.DataFrame(
        [(a, c, s) for a, cs in CANDIDATES.items() for c, s, _ in cs],
        columns=["anchor", "candidate", "score"],
    )
    pop = {"global": POP["pop:global"], "category": {"brinquedos": POP["pop:category:brinquedos"]}}
    offline = session_pools(
        anchors, anchors, lists, pop, pd.Series({"s1": "brinquedos"}), size=3, pop_size=2
    )
    assert X["product_id"].tolist() == offline["product_id"].tolist()
    assert not {"p1", "p2"} & set(X["product_id"])  # already viewed
    assert (X["last_product_ids"] == "p2,p1").all()
    assert X.set_index("product_id").loc["b", "views_24h"] == 30.0
    assert (X["n_events"] == 6.0).all()


def test_session_pool_equals_the_offline_session_pools() -> None:
    # the per-request port must reproduce candidates.session_pools exactly (ties included)
    rng = np.random.default_rng(0)
    items = [f"p{i}" for i in range(30)]
    for _ in range(400):
        anchors = list(rng.choice(items, rng.integers(1, 6), replace=False))
        lists = {
            a: [
                (str(c), float(round(rng.uniform(0, 1), 1)))
                for c in rng.choice(items, rng.integers(0, 15), replace=False)
            ]
            for a in anchors
            if rng.uniform() < 0.8
        }
        pop = {
            "global": list(rng.choice(items, rng.integers(0, 20), replace=False)),
            "category": {
                c: list(rng.choice(items, rng.integers(1, 10), replace=False))
                for c in ("c1", "c2")
                if rng.uniform() < 0.7
            },
        }
        category = [None, "c1", "c2", "missing"][rng.integers(0, 4)]
        size, pop_size = int(rng.integers(0, 12)), int(rng.integers(0, 7))
        seen = pd.DataFrame({"session_id": "s", "product_id": anchors})
        frame = pd.DataFrame(
            [(a, c, s) for a, cs in lists.items() for c, s in cs],
            columns=["anchor", "candidate", "score"],
        )
        cats = pd.Series({"s": category}, dtype=object)
        offline = session_pools(seen, seen, frame, pop, cats, size, pop_size)
        online = session_pool(anchors, lists, pop, category, size, pop_size)
        assert online == offline["product_id"].tolist(), (anchors, lists, pop, category)


def test_rank_inputs_equal_the_artefact_model_inputs() -> None:
    ids = ["a", "b", "c"]
    row = {**SESSION, "n_cart_adds": None, "dwell_seconds": "120"}
    pop = FakeOnline({}).popularity(ids)
    frame = pd.DataFrame({"product_id": ids}).assign(
        **{c: row.get(c) for c in [*SESSION_COLUMNS, "last_product_ids"]}
    )
    frame[POPULARITY_COLUMNS] = pop.loc[ids, POPULARITY_COLUMNS].to_numpy()
    pd.testing.assert_frame_equal(rank_inputs(ids, row, pop), model_inputs(frame))


def test_new_item_without_candidate_list_gets_popularity_candidates(ranker: StubRanker) -> None:
    online = FakeOnline({"s1": {**SESSION, "last_product_ids": "new-item"}})
    with _client(recommender=Recommender("4", ranker, 3, 2), online=online) as c:
        body = c.post("/recommend", json={"session_id": "s1", "k": 2}).json()
    # no cand:new-item -> the pool is the category's popularity (p2 was never viewed here)
    assert body["strategy"] == "rerank"
    assert [i["product_id"] for i in body["items"]] == ["t1", "p2"]


def test_unknown_session_gets_global_popularity(client: TestClient) -> None:
    body = client.post("/recommend", json={"session_id": "never-seen"}).json()
    assert body["strategy"] == "global_popularity" and body["model_version"] is None
    assert [i["product_id"] for i in body["items"]] == POP["pop:global"][:10]
    scores = [i["score"] for i in body["items"]]
    assert scores == sorted(scores, reverse=True)


def test_session_without_products_gets_its_category_popularity(ranker: StubRanker) -> None:
    online = FakeOnline({"s1": {**SESSION, "last_product_ids": None}})
    with _client(recommender=Recommender("4", ranker, 3, 2), online=online) as c:
        body = c.post("/recommend", json={"session_id": "s1", "k": 6}).json()
    assert body["strategy"] == "category_popularity"
    assert [i["product_id"] for i in body["items"]] == ["t1", "p2", "t2", "t3", "g1", "g2"]
    assert not ranker.inputs


def test_no_champion_falls_back_to_popularity_without_viewed_products() -> None:
    with _client(recommender=None) as c:
        body = c.post("/recommend", json={"session_id": "s1", "k": 4}).json()
    assert body["strategy"] == "category_popularity" and body["model_version"] is None
    assert [i["product_id"] for i in body["items"]] == ["t1", "t2", "t3", "g1"]


def test_category_without_popularity_list_falls_back_to_global() -> None:
    online = FakeOnline({"s1": {**SESSION, "last_category": "nova", "last_product_ids": None}})
    with _client(recommender=None, online=online) as c:
        body = c.post("/recommend", json={"session_id": "s1", "k": 3}).json()
    assert body["strategy"] == "global_popularity"
    assert [i["product_id"] for i in body["items"]] == ["g1", "g2", "p1"]


def test_short_rerank_pool_is_padded_with_popularity(ranker: StubRanker) -> None:
    body_k = 12
    with _client(recommender=Recommender("4", ranker, 3, 2)) as c:
        body = c.post("/recommend", json={"session_id": "s1", "k": body_k}).json()
    ids = [i["product_id"] for i in body["items"]]
    assert body["strategy"] == "rerank" and len(ids) == body_k == len(set(ids))
    assert not {"p1", "p2"} & set(ids)


def test_feature_store_error_degrades_to_popularity_not_500(ranker: StubRanker) -> None:
    with _client(recommender=Recommender("4", ranker, 3, 2), online=FakeOnline({}, True)) as c:
        r = c.post("/recommend", json={"session_id": "s1"})
    assert r.status_code == 200 and r.json()["strategy"] == "global_popularity"


def test_redis_down_is_503(ranker: StubRanker) -> None:
    with _client(recommender=Recommender("4", ranker, 3, 2), down=True) as c:
        r = c.post("/recommend", json={"session_id": "s1"})
    assert r.status_code == 503 and "redis" in r.json()["detail"].lower()


def test_unpublished_candidates_are_503_never_an_empty_list() -> None:
    with _client(recommender=None, redis_values={}) as c:
        r = c.post("/recommend", json={"session_id": "s1"})
    assert r.status_code == 503 and "publish-candidates" in r.json()["detail"]


@pytest.mark.parametrize(
    "body",
    [
        {"session_id": "s1", "k": 0},
        {"session_id": "s1", "k": 51},
        {"k": 3},
        {"session_id": "s1", "x": 1},
    ],
)
def test_request_contract(client: TestClient, body: dict[str, Any]) -> None:
    assert client.post("/recommend", json=body).status_code == 422


def _samples(text: str) -> dict[tuple[str, frozenset[tuple[str, str]]], float]:
    return {
        (s.name, frozenset(s.labels.items())): s.value
        for family in text_string_to_metric_families(text)
        for s in family.samples
    }


def test_recommend_metrics_by_strategy(client: TestClient) -> None:
    def calls() -> None:
        client.post("/recommend", json={"session_id": "s1"})
        client.post("/recommend", json={"session_id": "unknown"})

    calls()
    before = _samples(client.get("/metrics").text)
    calls()
    text = client.get("/metrics").text
    after = _samples(text)

    def delta(name: str, **labels: str) -> float:
        key = (name, frozenset(labels.items()))
        return after[key] - before.get(key, 0.0)

    assert delta("recommend_requests_total", strategy="rerank") == 1
    assert delta("recommend_requests_total", strategy="global_popularity") == 1
    assert "# TYPE recommend_latency_seconds histogram" in text
    assert delta("recommend_latency_seconds_count") == 2


def test_reload_picks_up_a_new_recommender(ranker: StubRanker) -> None:
    versions = iter([None, Recommender("5", ranker, 3, 2)])
    sources = Sources(FakeOnline({"s1": SESSION}), FakeRedis({**POP}), {})
    app = create_app(
        load_model=lambda: LoadedModel("1", ranker),
        load_lookup=lambda: lambda seller_id: {},
        load_recommender=lambda: next(versions),
        load_sources=lambda: sources,
    )
    with TestClient(app) as c:
        assert c.post("/recommend", json={"session_id": "s1"}).json()["model_version"] is None
        assert c.post("/reload").status_code == 200
        assert c.post("/recommend", json={"session_id": "s1"}).json()["model_version"] == "5"


def _enriched(root: Path, rows: list[dict[str, Any]]) -> None:
    schema = pa.schema(
        [("product_id", pa.string()), ("title", pa.string()), ("enriched_at", TS_UTC)]
    )
    write_deltalake(
        str(root / "silver" / "product_enriched"),
        pa.Table.from_pylist(rows, schema=schema),
        mode="append",
    )


def test_titles_are_the_latest_enrichment_per_product(tmp_path: Path) -> None:
    t0, t1 = pd.Timestamp("2026-10-01", tz="UTC"), pd.Timestamp("2026-10-02", tz="UTC")
    _enriched(tmp_path, [{"product_id": "a", "title": "Old car", "enriched_at": t1}])
    _enriched(
        tmp_path,
        [
            {"product_id": "a", "title": "Toy car", "enriched_at": t1 + pd.Timedelta("1s")},
            {"product_id": "b", "title": "Baby bottle", "enriched_at": t0},
            {"product_id": "c", "title": None, "enriched_at": t0},
        ],
    )
    assert load_titles(str(tmp_path)) == {"a": "Toy car", "b": "Baby bottle"}


def test_titles_are_empty_without_the_enriched_table(tmp_path: Path) -> None:
    assert load_titles(str(tmp_path)) == {}
    assert load_titles(str(tmp_path / "not-a-dir")) == {}


def test_items_carry_enriched_titles_null_when_absent(ranker: StubRanker) -> None:
    titles = {"c": "Toy car", "zzz": "unrelated"}
    with _client(recommender=Recommender("4", ranker, 3, 2), titles=titles) as c:
        body = c.post("/recommend", json={"session_id": "s1", "k": 3}).json()
    assert [(i["product_id"], i["title"]) for i in body["items"]] == [
        ("c", "Toy car"),
        ("t2", None),
        ("b", None),
    ]


@pytest.mark.parametrize("champion", [True, False])
def test_reload_refreshes_titles(ranker: StubRanker, champion: bool) -> None:
    titles = iter([{}, {"g1": "Garden hose"}])
    app = create_app(
        load_model=lambda: LoadedModel("1", ranker) if champion else None,
        load_lookup=lambda: lambda seller_id: {},
        load_recommender=lambda: None,
        load_sources=lambda: Sources(None, FakeRedis({**POP}), {}, next(titles)),
    )
    with TestClient(app) as c:
        assert c.post("/recommend", json={"session_id": "x"}).json()["items"][0]["title"] is None
        # titles refresh even when there is no late_delivery champion to reload (503)
        assert c.post("/reload").status_code == (200 if champion else 503)
        item = c.post("/recommend", json={"session_id": "x"}).json()["items"][0]
    assert item == {
        "product_id": "g1",
        "score": 1.0,
        "category": None,
        "category_en": None,
        "title": "Garden hose",
    }


def test_online_reads_are_not_ttl_filtered_against_dataset_time(tmp_path: Path) -> None:
    # pushed rows carry 2017 dataset time; Feast's online read must still return them
    store = local_store(tmp_path)
    old = pd.Timestamp("2017-03-01 10:00", tz="UTC")
    store.push(
        "session_push",
        pd.DataFrame([{"session_id": "s1", **SESSION, "session_start_ts": old, "event_ts": old}]),
        to=PushMode.ONLINE,
    )
    store.push(
        "popularity_push",
        pd.DataFrame(
            {
                "product_id": ["b"],
                "views_1h": [3],
                "views_24h": [30],
                "carts_24h": [2],
                "event_ts": [old],
            }
        ),
        to=PushMode.ONLINE,
    )
    online = FeastOnline(store)
    row = online.session("s1")
    assert row is not None and row["last_product_ids"] == "p2,p1" and row["n_events"] == 6
    assert online.session("never-seen") is None
    pop = online.popularity(["b", "unknown"])
    assert pop.loc["b", "views_24h"] == 30.0 and pop.loc["unknown", "views_24h"] == 0.0
    assert pop.loc["b", "carts_24h"] == 2.0


def test_load_recommender_resolves_champion_and_pool_sizes(mlflow_uri: str) -> None:
    assert load_recommender() is None
    model = RecommenderModel(tables=None, ranker=None, pool_size=7, pop_pool=3)  # type: ignore[arg-type]
    with mlflow.start_run():
        info = mlflow.pyfunc.log_model(name="model", python_model=model)
    mlflow.register_model(info.model_uri, "recommender")
    MlflowClient().set_registered_model_alias("recommender", "champion", "1")
    loaded = load_recommender()
    assert loaded is not None
    assert (loaded.version, loaded.pool_size, loaded.pop_pool) == ("1", 7, 3)


def test_single_threaded_covers_the_recommender_ranker(tmp_path: Path) -> None:
    ranker = lgb.LGBMRanker(n_estimators=2, n_jobs=4)
    model = RecommenderModel(tables=None, ranker=ranker, pool_size=1, pop_pool=1)  # type: ignore[arg-type]
    mlflow.pyfunc.save_model(tmp_path / "m", python_model=model)
    loaded = mlflow.pyfunc.load_model(str(tmp_path / "m"))
    single_threaded(loaded)
    assert loaded.unwrap_python_model().ranker.get_params()["n_jobs"] == 1
