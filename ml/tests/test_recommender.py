import fnmatch
import json
from typing import Any

import numpy as np
import pandas as pd
import pytest

from retail_ml.recommender.candidates import (
    candidate_lists,
    covisitation,
    fit_als,
    interactions,
    popularity,
    session_pools,
)
from retail_ml.recommender.publish import candidate_payloads, publish
from retail_ml.recommender.ranker import session_prefixes
from retail_ml.recommender.train import ranking_metrics

COLUMNS = [
    "event_id",
    "event_type",
    "session_id",
    "customer_id",
    "device",
    "referrer",
    "event_ts",
    "product_id",
    "search_query",
    "quantity",
    "order_id",
    "is_purchase_target",
]


def events(rows: list[tuple[str, str, str | None, int]], purchased: set[tuple[str, str]] = set()):  # noqa: B006
    """(session, event_type, product_id, second) rows in the offline sessions contract."""
    base = pd.Timestamp("2018-01-01 10:00:00")
    out = []
    for i, (session, event_type, product, second) in enumerate(rows):
        out.append(
            {
                "event_id": f"e{i}",
                "event_type": event_type,
                "session_id": session,
                "customer_id": None,
                "device": "mobile",
                "referrer": None,
                "event_ts": (base + pd.Timedelta(seconds=second)).strftime("%Y-%m-%dT%H:%M:%S"),
                "product_id": product,
                "search_query": None,
                "quantity": 1 if event_type == "add_to_cart" else None,
                "order_id": "o1" if event_type == "checkout_started" else None,
                "is_purchase_target": (session, product) in purchased,
            }
        )
    df = pd.DataFrame(out, columns=COLUMNS)
    df["event_ts"] = pd.to_datetime(df["event_ts"], utc=True)
    return df


ITEMS = pd.Index(["A", "B", "C", "D"])


def test_covisitation_weights_one_over_distance_in_session_order() -> None:
    df = events(
        [
            ("s1", "product_view", "A", 0),
            ("s1", "page_view", None, 1),  # not a product event: no position
            ("s1", "product_view", "B", 2),
            ("s1", "add_to_cart", "C", 3),
            ("s2", "product_view", "B", 0),
            ("s2", "product_view", "A", 1),
            ("s3", "product_view", "A", 0),
            ("s3", "product_view", "A", 1),  # same product: no self pair, still a position
            ("s3", "product_view", "B", 2),
        ]
    ).sample(frac=1, random_state=0)  # file order must not matter, event_ts does
    m = covisitation(df, ITEMS).toarray()
    a, b, c, d = range(4)
    assert m[a, b] == m[b, a] == pytest.approx(1 + 1 + 1 + 0.5)
    assert m[a, c] == m[c, a] == pytest.approx(0.5)
    assert m[b, c] == pytest.approx(1.0)
    assert np.diag(m).sum() == 0 and m[d].sum() == 0


def test_interactions_take_the_strongest_signal_per_session_product() -> None:
    df = events(
        [
            ("s1", "product_view", "A", 0),
            ("s1", "add_to_cart", "A", 1),
            ("s1", "product_view", "B", 2),
            ("s2", "product_view", "C", 0),
            ("s2", "add_to_cart", "C", 1),
            ("s2", "checkout_started", None, 2),
        ],
        purchased={("s2", "C")},
    )
    m = interactions(df, ITEMS).toarray()
    assert m.shape == (2, 4)
    assert m.tolist() == [[3.0, 1.0, 0.0, 0.0], [0.0, 0.0, 5.0, 0.0]]


def test_als_item_factors_shape_and_unseen_items_get_no_neighbours() -> None:
    rng = np.random.default_rng(0)
    rows = [
        (f"s{s}", "product_view", str(p), i)
        for s in range(60)
        for i, p in enumerate(rng.choice(["A", "B", "C"], 3))
    ]
    m = interactions(events(rows), ITEMS)
    factors = fit_als(m, {"factors": 8, "iterations": 5, "regularization": 0.1, "random_state": 0})
    assert factors.shape == (4, 8)
    assert np.isfinite(factors).all()
    assert not factors[3].any()  # D never interacted with


def test_candidate_lists_merge_sources_and_pad_new_items_with_popularity() -> None:
    df = events(
        [
            ("s1", "product_view", "A", 0),
            ("s1", "product_view", "B", 1),
            ("s2", "product_view", "A", 0),
            ("s2", "product_view", "B", 1),
            ("s3", "product_view", "B", 0),
            ("s3", "product_view", "C", 1),
        ]
    )
    category = pd.Series({"A": "x", "B": "x", "C": "y", "D": "x"})
    pop = popularity(df, category, n=100)
    assert pop["global"] == ["B", "A", "C"]
    assert pop["category"] == {"x": ["B", "A"], "y": ["C"]}
    factors = np.array([[1.0, 0.0], [0.9, 0.1], [0.0, 1.0], [0.0, 0.0]])
    lists = candidate_lists(covisitation(df, ITEMS), factors, ITEMS, category, pop, n=3)
    a = lists[lists["anchor"] == "A"].reset_index(drop=True)
    assert a["candidate"].tolist()[0] == "B"
    assert a.loc[0, "source"] == "covis+als"
    assert a.loc[0, "score"] == pytest.approx(1.0 + 0.9 / np.hypot(0.9, 0.1))
    assert set(a["candidate"]) == {"B", "C"}  # D: no covis, zero ALS vector, never popular
    # Review Focus 3: D never appeared in a session -> category, then global, popularity
    d = lists[lists["anchor"] == "D"]
    assert d["candidate"].tolist() == ["B", "A", "C"]
    assert set(d["source"]) == {"popularity"}
    assert (lists.groupby("anchor").size() <= 3).all()
    assert not (lists["anchor"] == lists["candidate"]).any()


def test_session_pools_union_anchors_and_popularity_excluding_viewed() -> None:
    lists = pd.DataFrame(
        {
            "anchor": ["A", "A", "B", "B"],
            "candidate": ["B", "C", "C", "D"],
            "score": [1.0, 0.5, 0.6, 0.2],
            "source": ["covis", "als", "covis", "covis"],
        }
    )
    anchors = pd.DataFrame(
        {"session_id": ["s1", "s1", "s2", "s3"], "product_id": ["A", "B", "unknown_new", None]}
    )
    pop = {"global": ["D", "C", "A"], "category": {"x": ["E", "C"]}}
    last_category = pd.Series({"s1": "x", "s2": None, "s3": "no_list"})
    pools = session_pools(anchors, anchors, lists, pop, last_category, size=10, pop_size=2)
    s1 = pools[pools["session_id"] == "s1"]
    # C reached from both anchors (0.5 + 0.6), B and A excluded as already viewed, then the
    # category's popular products (C already pooled), score 0
    assert s1["product_id"].tolist() == ["C", "D", "E"]
    assert s1["cand_score"].tolist() == pytest.approx([1.1, 0.2, 0.0])
    # new product / no product at all / category without a list: global popularity, never empty
    for sid in ("s2", "s3"):
        assert pools.loc[pools["session_id"] == sid, "product_id"].tolist() == ["D", "C"]


def test_prefix_stops_before_the_last_product_view_preceding_checkout() -> None:
    df = events(
        [
            ("s1", "page_view", None, 0),
            ("s1", "product_view", "A", 10),
            ("s1", "product_view", "B", 20),
            ("s1", "product_view", "C", 30),
            ("s1", "add_to_cart", "C", 40),
            ("s1", "checkout_started", None, 50),
            ("s1", "add_to_cart", "D", 45),
            ("browse", "product_view", "A", 0),
        ],
        purchased={("s1", "C"), ("s1", "A"), ("s1", "D")},
    )
    prefix, targets = session_prefixes(df)
    assert prefix["product_id"].tolist() == [None, "A", "B"]
    # A was ordered but already viewed in the prefix: never recommendable, so not a target
    assert targets.to_dict("records") == [
        {"session_id": "s1", "product_id": "C"},
        {"session_id": "s1", "product_id": "D"},
    ]


def test_ranking_metrics_by_hand() -> None:
    ranked = pd.DataFrame(
        {
            "session_id": ["s1", "s1", "s1", "s2", "s2"],
            "product_id": ["A", "B", "C", "A", "D"],
            "score": [3.0, 2.0, 1.0, 2.0, 1.0],
        }
    )
    targets = pd.DataFrame(
        {"session_id": ["s1", "s2", "s2", "s3"], "product_id": ["B", "D", "E", "A"]}
    )
    m = ranking_metrics(ranked, targets, k=2, catalogue_size=10)
    assert m["recall_at_2"] == pytest.approx((1 + 0.5 + 0) / 3)
    ndcg_s1 = (1 / np.log2(3)) / 1.0
    ndcg_s2 = (1 / np.log2(3)) / (1 + 1 / np.log2(3))
    assert m["ndcg_at_2"] == pytest.approx((ndcg_s1 + ndcg_s2 + 0) / 3)
    assert m["coverage_at_2"] == pytest.approx(3 / 10)  # A, B, D in some top-2 (C is rank 3)


class FakeRedis:
    def __init__(self) -> None:
        self.data: dict[str, str] = {}

    def pipeline(self, transaction: bool = True) -> "FakeRedis":
        return self

    def set(self, key: str, value: str) -> None:
        self.data[key] = value

    def delete(self, *keys: str) -> None:
        for key in keys:
            self.data.pop(key, None)

    def execute(self) -> list[Any]:
        return []

    def scan_iter(self, match: str, count: int = 1000) -> list[str]:
        return [k for k in list(self.data) if fnmatch.fnmatch(k, match)]


def test_publish_writes_contract_keys_and_drops_stale_ones() -> None:
    lists = pd.DataFrame(
        {
            "anchor": ["A", "A", "B"],
            "candidate": ["B", "C", "A"],
            "score": [1.23456789, 0.5, 0.7],
            "source": ["covis+als", "als", "popularity"],
        }
    )
    pop = {"global": ["B", "A"], "category": {"x": ["B"]}}
    fake = FakeRedis()
    fake.data |= {"cand:gone": "[]", "pop:category:gone": "[]", "feast-key": "keep"}
    counts = publish(fake, candidate_payloads(lists, pop))
    assert json.loads(fake.data["cand:A"]) == [["B", 1.234568, "covis+als"], ["C", 0.5, "als"]]
    assert json.loads(fake.data["cand:B"]) == [["A", 0.7, "popularity"]]
    assert json.loads(fake.data["pop:global"]) == ["B", "A"]
    assert json.loads(fake.data["pop:category:x"]) == ["B"]
    assert "cand:gone" not in fake.data and "pop:category:gone" not in fake.data
    assert fake.data["feast-key"] == "keep"
    assert counts == {"candidate_keys": 2, "popularity_keys": 2, "stale_deleted": 2}
