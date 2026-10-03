import random
from dataclasses import dataclass, replace

from clickstream_sim.events import Event

HOUR_SECONDS = 3600
CORRUPTIONS = (
    "event_id_null",
    "session_id_null",
    "ts_unparseable",
    "ts_epoch",
    "ts_future",
    "negative_quantity",
)


@dataclass(frozen=True)
class FaultConfig:
    late_rate: float = 0.05
    late_max_hours: int = 48
    dup_rate: float = 0.01
    bad_rate: float = 0.0


@dataclass(frozen=True)
class Emission:
    event: Event
    delay_s: float
    is_duplicate: bool
    schema_version: int


def _corrupt(event: Event, rng: random.Random) -> Event:
    options = [
        c for c in CORRUPTIONS if c != "negative_quantity" or event.event_type == "add_to_cart"
    ]
    match rng.choice(options):
        case "event_id_null":
            return replace(event, event_id=None)
        case "session_id_null":
            return replace(event, session_id=None)
        case "ts_unparseable":
            return replace(event, event_ts="not-a-timestamp")
        case "ts_epoch":
            return replace(event, event_ts="1970-01-01T00:00:00")
        case "ts_future":
            return replace(event, event_ts="2099-12-31T00:00:00")
        case _:
            return replace(event, quantity=-1)


def inject(
    events: list[Event], cfg: FaultConfig, rng: random.Random, schema_version: int
) -> list[Emission]:
    emissions: list[Emission] = []
    for source in events:
        late = rng.random() < cfg.late_rate
        bad = rng.random() < cfg.bad_rate
        duplicate = rng.random() < cfg.dup_rate
        delay = rng.uniform(HOUR_SECONDS, cfg.late_max_hours * HOUR_SECONDS) if late else 0.0
        event = _corrupt(source, rng) if bad else source
        emissions.append(Emission(event, delay, False, schema_version))
        if duplicate:
            emissions.append(Emission(event, delay + 1, True, schema_version))
    return emissions
