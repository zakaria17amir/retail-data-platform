import io
import json
import random
import urllib.error
import urllib.request
from typing import Any

import pytest
from clickstream_sim.events import Event
from clickstream_sim.feedback import (
    CLICK_BASE,
    Recommendations,
    click_probability,
    feedback_events,
    fetch_recommendations,
)

TRIGGER = Event(
    event_id="e-7",
    event_type="product_view",
    session_id="s1",
    customer_id="c1",
    device="mobile",
    referrer="google",
    event_ts="2017-10-02T10:00:00",
    product_id="p1",
    search_query=None,
    quantity=None,
    order_id=None,
)
RECS = Recommendations(
    items=(("p2", "toys"), ("p3", "books"), ("p4", None)),
    model_version="3",
    strategy="rerank",
)


class _Response(io.BytesIO):
    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


def _opener(body: Any, calls: list[tuple[urllib.request.Request, float]]) -> Any:
    def opener(request: urllib.request.Request, timeout: float) -> _Response:
        calls.append((request, timeout))
        if isinstance(body, BaseException):
            raise body
        return _Response(body if isinstance(body, bytes) else json.dumps(body).encode())

    return opener


def test_fetch_posts_session_and_k_with_200ms_timeout() -> None:
    calls: list[tuple[urllib.request.Request, float]] = []
    body = {
        "session_id": "s1",
        "items": [
            {"product_id": "p2", "score": 0.9, "category": "toys", "title": None},
            {"product_id": "p3", "score": 0.5, "category": None, "title": None},
        ],
        "model_version": "3",
        "strategy": "rerank",
    }
    recs = fetch_recommendations("http://serving:8000/", "s1", opener=_opener(body, calls))
    assert recs == Recommendations((("p2", "toys"), ("p3", None)), "3", "rerank")
    request, timeout = calls[0]
    assert request.full_url == "http://serving:8000/recommend"
    assert request.get_method() == "POST"
    assert json.loads(request.data) == {"session_id": "s1", "k": 10}  # type: ignore[arg-type]
    assert timeout == 0.2


@pytest.mark.parametrize(
    "body",
    [
        urllib.error.URLError("refused"),
        TimeoutError("timed out"),
        urllib.error.HTTPError("http://x/recommend", 500, "boom", {}, None),  # type: ignore[arg-type]
        b"not json",
        {"detail": "no items key"},
        {"items": ["p2"]},
    ],
)
def test_fetch_failure_returns_none(body: Any) -> None:
    assert fetch_recommendations("http://x", "s1", opener=_opener(body, [])) is None


def test_feedback_events_carry_the_trigger_dataset_time_and_session() -> None:
    events = feedback_events(TRIGGER, RECS, "toys", random.Random(1))
    shown = [e for e in events if e.event_type == "recommendation_shown"]
    assert [(e.product_id, e.rank) for e in shown] == [("p2", 1), ("p3", 2), ("p4", 3)]
    assert {e.event_ts for e in events} == {TRIGGER.event_ts}
    assert {(e.session_id, e.customer_id, e.device, e.referrer) for e in events} == {
        ("s1", "c1", "mobile", "google")
    }
    assert {(e.rec_model_version, e.rec_strategy) for e in events} == {("3", "rerank")}
    assert len({e.event_id for e in events}) == len(events)
    assert set(e.event_type for e in events) <= {"recommendation_shown", "recommendation_clicked"}
    assert feedback_events(TRIGGER, RECS, "toys", random.Random(1)) == events


def test_click_probability_is_base_over_rank_doubled_on_category_match() -> None:
    assert CLICK_BASE == 0.3
    assert click_probability(1, same_category=False) == pytest.approx(0.3)
    assert click_probability(3, same_category=False) == pytest.approx(0.1)
    assert click_probability(2, same_category=True) == pytest.approx(0.3)


def test_click_rates_follow_the_probability_with_a_fixed_seed() -> None:
    rng = random.Random(42)
    trials = 20_000
    recs = Recommendations((("p2", "toys"), ("p3", "books")), None, None)
    clicks = {1: 0, 2: 0}
    for _ in range(trials):
        for e in feedback_events(TRIGGER, recs, "books", rng):
            if e.event_type == "recommendation_clicked":
                assert e.rank is not None
                clicks[e.rank] += 1
    assert clicks[1] / trials == pytest.approx(0.3, abs=0.015)
    assert clicks[2] / trials == pytest.approx(0.3, abs=0.015)
    assert clicks == {1: 5984, 2: 5925}
