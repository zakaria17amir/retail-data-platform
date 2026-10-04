import json
import random
import urllib.request
import uuid
from collections.abc import Callable
from dataclasses import dataclass, replace
from http.client import HTTPException
from typing import Any

from clickstream_sim.events import EVENT_ID_NAMESPACE, Event

CLICK_BASE = 0.3
CATEGORY_BOOST = 2.0
RECOMMEND_K = 10
RECOMMEND_TIMEOUT_S = 0.2


@dataclass(frozen=True)
class Recommendations:
    items: tuple[tuple[str, str | None], ...]
    model_version: str | None
    strategy: str | None


def fetch_recommendations(
    base_url: str,
    session_id: str,
    *,
    k: int = RECOMMEND_K,
    timeout_s: float = RECOMMEND_TIMEOUT_S,
    opener: Callable[..., Any] = urllib.request.urlopen,
) -> Recommendations | None:
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/recommend",
        data=json.dumps({"session_id": session_id, "k": k}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with opener(request, timeout=timeout_s) as response:
            body = json.load(response)
        items = tuple((str(i["product_id"]), i.get("category")) for i in body["items"][:k])
        version, strategy = body.get("model_version"), body.get("strategy")
    except (OSError, HTTPException, ValueError, KeyError, TypeError, AttributeError):
        return None
    return Recommendations(
        items,
        None if version is None else str(version),
        None if strategy is None else str(strategy),
    )


def click_probability(rank: int, *, same_category: bool) -> float:
    return CLICK_BASE / rank * (CATEGORY_BOOST if same_category else 1.0)


def _feedback(
    trigger: Event, kind: str, rank: int, product_id: str, recs: Recommendations
) -> Event:
    return replace(
        trigger,
        event_id=str(uuid.uuid5(EVENT_ID_NAMESPACE, f"{trigger.event_id}:{kind}:{rank}")),
        event_type=f"recommendation_{kind}",
        product_id=product_id,
        search_query=None,
        quantity=None,
        order_id=None,
        rank=rank,
        rec_model_version=recs.model_version,
        rec_strategy=recs.strategy,
    )


def feedback_events(
    trigger: Event, recs: Recommendations, session_category: str | None, rng: random.Random
) -> list[Event]:
    shown = [_feedback(trigger, "shown", r, p, recs) for r, (p, _) in enumerate(recs.items, 1)]
    clicked = [
        _feedback(trigger, "clicked", rank, product_id, recs)
        for rank, (product_id, category) in enumerate(recs.items, 1)
        if rng.random()
        < click_probability(
            rank, same_category=category is not None and category == session_category
        )
    ]
    return shown + clicked
