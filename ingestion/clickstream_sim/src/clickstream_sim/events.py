import hashlib
import itertools
import random
import uuid
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, fields
from datetime import datetime, timedelta
from typing import TypedDict

EVENT_TYPES = ("page_view", "search", "product_view", "add_to_cart", "checkout_started")
DEVICES = ("mobile", "desktop", "tablet")
REFERRERS = ("direct", "google", "instagram", "email", None)

TS_FORMAT = "%Y-%m-%dT%H:%M:%S"
SESSION_WINDOW_SECONDS = 30 * 60
BROWSING_JITTER_S = 3600
V3_FIELDS = ("rank", "rec_model_version", "rec_strategy")
EVENT_ID_NAMESPACE = uuid.UUID("5b0f8c1e-2d4a-4c6e-9f3b-7a1d2e8c4b90")


@dataclass(frozen=True)
class Event:
    event_id: str | None
    event_type: str
    session_id: str | None
    customer_id: str | None
    device: str
    referrer: str | None
    event_ts: str
    product_id: str | None
    search_query: str | None
    quantity: int | None
    order_id: str | None
    utm_campaign: str | None = None
    rank: int | None = None
    rec_model_version: str | None = None
    rec_strategy: str | None = None

    def to_dict(self, schema_version: int) -> dict[str, object]:
        record = {f.name: getattr(self, f.name) for f in fields(self)}
        if schema_version < 3:
            for name in V3_FIELDS:
                del record[name]
        if schema_version < 2:
            del record["utm_campaign"]
        return record


@dataclass(frozen=True)
class Catalogue:
    by_category: dict[str, tuple[str, ...]]
    category_of: dict[str, str]
    all_products: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.all_products:
            known = set(self.category_of).union(*self.by_category.values())
            object.__setattr__(self, "all_products", tuple(sorted(known)))

    @classmethod
    def from_rows(cls, rows: Iterable[tuple[str, str | None]]) -> "Catalogue":
        grouped: dict[str, list[str]] = defaultdict(list)
        products: set[str] = set()
        for product_id, category in rows:
            products.add(product_id)
            if category:
                grouped[category].append(product_id)
        by_category = {c: tuple(sorted(ps)) for c, ps in sorted(grouped.items())}
        category_of = {p: c for c, ps in by_category.items() for p in ps}
        return cls(by_category, category_of, tuple(sorted(products)))

    def random_product(self, rng: random.Random, category: str | None) -> str:
        candidates = self.by_category.get(category, ()) if category is not None else ()
        pool = candidates or self.all_products
        if not pool:
            raise ValueError("empty catalogue")
        return rng.choice(pool)


@dataclass(frozen=True)
class OrderRef:
    order_id: str
    customer_id: str | None
    purchase_ts: datetime
    product_ids: tuple[str, ...]


def _session_id(key: str) -> str:
    return hashlib.sha1(key.encode()).hexdigest()[:16]


def _search_query(category: str | None) -> str:
    return category.replace("_", " ") if category else "offers"


class _Extra(TypedDict, total=False):
    product_id: str | None
    search_query: str | None
    quantity: int | None
    order_id: str | None


class _Builder:
    def __init__(self, rng: random.Random, session_id: str, customer_id: str | None) -> None:
        self.rng = rng
        self.session_id = session_id
        self.customer_id = customer_id
        self.device = rng.choice(DEVICES)
        self.referrer = rng.choice(REFERRERS)
        self.steps = itertools.count()

    def make(
        self,
        event_type: str,
        at: datetime,
        *,
        product_id: str | None = None,
        search_query: str | None = None,
        quantity: int | None = None,
        order_id: str | None = None,
    ) -> Event:
        step = next(self.steps)
        return Event(
            event_id=str(uuid.uuid5(EVENT_ID_NAMESPACE, f"{self.session_id}:{step}")),
            event_type=event_type,
            session_id=self.session_id,
            customer_id=self.customer_id,
            device=self.device,
            referrer=self.referrer,
            event_ts=at.strftime(TS_FORMAT),
            product_id=product_id,
            search_query=search_query,
            quantity=quantity,
            order_id=order_id,
        )


def converting_session(order: OrderRef, catalogue: Catalogue, rng: random.Random) -> list[Event]:
    builder = _Builder(rng, _session_id(order.order_id), order.customer_id)
    purchased = rng.choice(order.product_ids)
    category = catalogue.category_of.get(purchased)

    steps: list[tuple[str, _Extra]] = [("page_view", {})]
    if rng.random() < 0.5:
        steps.append(("search", {"search_query": _search_query(category)}))
    viewed = [catalogue.random_product(rng, category) for _ in range(rng.randint(1, 4))]
    steps.extend(("product_view", {"product_id": p}) for p in [*viewed, purchased])
    steps.extend(("add_to_cart", {"product_id": p, "quantity": 1}) for p in order.product_ids)
    steps.append(("checkout_started", {"order_id": order.order_id}))

    start = order.purchase_ts.replace(microsecond=0) - timedelta(seconds=SESSION_WINDOW_SECONDS)
    offsets = sorted(rng.sample(range(SESSION_WINDOW_SECONDS + 1), len(steps)))
    return [
        builder.make(event_type, start + timedelta(seconds=offset), **extra)
        for (event_type, extra), offset in zip(steps, offsets, strict=True)
    ]


def browsing_session(
    at: datetime, catalogue: Catalogue, rng: random.Random, key: str
) -> list[Event]:
    builder = _Builder(rng, _session_id(key), None)
    first = catalogue.random_product(rng, None)
    category = catalogue.category_of.get(first)

    steps: list[tuple[str, _Extra]] = [("page_view", {})]
    if rng.random() < 0.5:
        steps.append(("search", {"search_query": _search_query(category)}))
    viewed = [first, *(catalogue.random_product(rng, category) for _ in range(rng.randint(0, 3)))]
    steps.extend(("product_view", {"product_id": p}) for p in viewed)
    if rng.random() < 0.5:
        steps.append(("add_to_cart", {"product_id": viewed[-1], "quantity": 1}))

    events: list[Event] = []
    moment = at.replace(microsecond=0)
    for event_type, extra in steps:
        events.append(builder.make(event_type, moment, **extra))
        moment += timedelta(seconds=rng.randint(5, 90))
    return events


def order_sessions(
    order: OrderRef, catalogue: Catalogue, rng: random.Random, browsing_ratio: int
) -> list[list[Event]]:
    sessions = [converting_session(order, catalogue, rng)]
    for i in range(browsing_ratio):
        jitter = timedelta(seconds=rng.randint(-BROWSING_JITTER_S, BROWSING_JITTER_S))
        key = f"{order.order_id}:browse:{i}"
        sessions.append(browsing_session(order.purchase_ts + jitter, catalogue, rng, key))
    return sessions
