import hashlib
import random
import uuid
from dataclasses import dataclass, fields
from datetime import datetime, timedelta

EVENT_TYPES = ("page_view", "search", "product_view", "add_to_cart", "checkout_started")
DEVICES = ("mobile", "desktop", "tablet")
REFERRERS = ("direct", "google", "instagram", "email", None)

TS_FORMAT = "%Y-%m-%dT%H:%M:%S"
SESSION_WINDOW_SECONDS = 30 * 60


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

    def to_dict(self, schema_version: int) -> dict[str, object]:
        record = {f.name: getattr(self, f.name) for f in fields(self)}
        if schema_version < 2:
            del record["utm_campaign"]
        return record


@dataclass(frozen=True)
class Catalogue:
    by_category: dict[str, tuple[str, ...]]
    category_of: dict[str, str]

    def random_product(self, rng: random.Random, category: str | None) -> str:
        if category is not None and category in self.by_category:
            return rng.choice(self.by_category[category])
        return rng.choice(sorted(self.category_of))


@dataclass(frozen=True)
class OrderRef:
    order_id: str
    customer_id: str | None
    purchase_ts: datetime
    product_ids: tuple[str, ...]


def _event_id(rng: random.Random) -> str:
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def _search_query(category: str | None) -> str:
    return category.replace("_", " ") if category else "offers"


class _Builder:
    def __init__(self, rng: random.Random, session_id: str, customer_id: str | None) -> None:
        self.rng = rng
        self.session_id = session_id
        self.customer_id = customer_id
        self.device = rng.choice(DEVICES)
        self.referrer = rng.choice(REFERRERS)

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
        return Event(
            event_id=_event_id(self.rng),
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
    builder = _Builder(
        rng, hashlib.sha1(order.order_id.encode()).hexdigest()[:16], order.customer_id
    )
    purchased = rng.choice(order.product_ids)
    category = catalogue.category_of.get(purchased)

    steps: list[tuple[str, dict[str, object]]] = [("page_view", {})]
    if rng.random() < 0.5:
        steps.append(("search", {"search_query": _search_query(category)}))
    viewed = [catalogue.random_product(rng, category) for _ in range(rng.randint(1, 4))]
    steps.extend(("product_view", {"product_id": p}) for p in [*viewed, purchased])
    steps.extend(("add_to_cart", {"product_id": p, "quantity": 1}) for p in order.product_ids)
    steps.append(("checkout_started", {"order_id": order.order_id}))

    start = order.purchase_ts - timedelta(seconds=SESSION_WINDOW_SECONDS)
    offsets = sorted(rng.sample(range(SESSION_WINDOW_SECONDS + 1), len(steps)))
    return [
        builder.make(event_type, start + timedelta(seconds=offset), **extra)  # type: ignore[arg-type]
        for (event_type, extra), offset in zip(steps, offsets, strict=True)
    ]


def browsing_session(at: datetime, catalogue: Catalogue, rng: random.Random) -> list[Event]:
    session_id = hashlib.sha1(str(rng.getrandbits(64)).encode()).hexdigest()[:16]
    builder = _Builder(rng, session_id, None)
    first = catalogue.random_product(rng, None)
    category = catalogue.category_of.get(first)

    steps: list[tuple[str, dict[str, object]]] = [("page_view", {})]
    if rng.random() < 0.5:
        steps.append(("search", {"search_query": _search_query(category)}))
    viewed = [first, *(catalogue.random_product(rng, category) for _ in range(rng.randint(0, 3)))]
    steps.extend(("product_view", {"product_id": p}) for p in viewed)
    if rng.random() < 0.5:
        steps.append(("add_to_cart", {"product_id": viewed[-1], "quantity": 1}))

    events: list[Event] = []
    moment = at
    for event_type, extra in steps:
        events.append(builder.make(event_type, moment, **extra))  # type: ignore[arg-type]
        moment += timedelta(seconds=rng.randint(5, 90))
    return events
