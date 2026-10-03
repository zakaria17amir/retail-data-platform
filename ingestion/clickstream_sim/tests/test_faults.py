import random
from datetime import datetime

from clickstream_sim.events import Event
from clickstream_sim.faults import FaultConfig, inject


def _events(n: int) -> list[Event]:
    return [
        Event(
            event_id=f"e{i}",
            event_type="add_to_cart" if i % 2 else "page_view",
            session_id=f"s{i}",
            customer_id=None,
            device="mobile",
            referrer=None,
            event_ts=datetime(2017, 6, 1, 12, 0, i % 60).isoformat(timespec="seconds"),
            product_id="p1",
            search_query=None,
            quantity=1 if i % 2 else None,
            order_id=None,
        )
        for i in range(n)
    ]


def test_inject_rates_with_fixed_seed() -> None:
    events = _events(1000)
    cfg = FaultConfig(late_rate=0.1, dup_rate=0.1, bad_rate=0.1)
    emissions = inject(events, cfg, random.Random(42), schema_version=1)
    originals = [e for e in emissions if not e.is_duplicate]
    dups = [e for e in emissions if e.is_duplicate]
    late = [e for e in originals if e.delay_s >= 3600]
    bad = [e for e, source in zip(originals, events, strict=True) if e.event != source]
    for observed in (len(late), len(dups), len(bad)):
        assert 70 <= observed <= 130
    assert len(originals) == 1000
    assert all(e.delay_s <= 48 * 3600 for e in late)
    for dup in dups:
        assert any(dup.event is o.event and dup.delay_s == o.delay_s + 1 for o in originals)
    assert all(e.schema_version == 1 for e in emissions)


def test_bad_event_has_exactly_one_corruption() -> None:
    events = _events(300)
    emissions = inject(
        events, FaultConfig(late_rate=0, dup_rate=0, bad_rate=1.0), random.Random(1), 2
    )
    fields = [
        "event_id",
        "session_id",
        "event_ts",
        "quantity",
    ]
    seen: set[str] = set()
    for emission, source in zip(emissions, events, strict=True):
        changed = [f for f in fields if getattr(emission.event, f) != getattr(source, f)]
        assert len(changed) == 1
        seen.add(changed[0])
        if changed[0] == "quantity":
            assert source.event_type == "add_to_cart"
            assert emission.event.quantity == -1
        if changed[0] == "event_ts":
            assert emission.event.event_ts in {
                "not-a-timestamp",
                "1970-01-01T00:00:00",
                "2099-12-31T00:00:00",
            }
        others = [f for f in Event.__dataclass_fields__ if f not in fields]
        assert all(getattr(emission.event, f) == getattr(source, f) for f in others)
    assert seen == set(fields)


def test_zero_rates_identity() -> None:
    events = _events(50)
    emissions = inject(events, FaultConfig(0, 48, 0, 0), random.Random(0), schema_version=2)
    assert [e.event for e in emissions] == events
    assert all(e.event is s for e, s in zip(emissions, events, strict=True))
    assert all(e.delay_s == 0 and not e.is_duplicate and e.schema_version == 2 for e in emissions)


def test_inject_is_deterministic() -> None:
    events = _events(200)
    cfg = FaultConfig(late_rate=0.2, dup_rate=0.2, bad_rate=0.2)
    assert inject(events, cfg, random.Random(5), 1) == inject(events, cfg, random.Random(5), 1)
