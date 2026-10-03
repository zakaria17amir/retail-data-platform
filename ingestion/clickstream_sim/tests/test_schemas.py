import io
import json
from dataclasses import fields
from pathlib import Path

import fastavro
from clickstream_sim.events import Event

SCHEMAS = Path(__file__).resolve().parents[2] / "schemas" / "events"


def _load(version: int) -> dict[str, object]:
    schema: dict[str, object] = json.loads(
        (SCHEMAS / f"clickstream_event.v{version}.avsc").read_text(encoding="utf-8")
    )
    return schema


def _names(version: int) -> list[str]:
    return [f["name"] for f in _load(version)["fields"]]  # type: ignore[attr-defined]


def test_schemas_parse() -> None:
    for version in (1, 2):
        fastavro.parse_schema(_load(version))


def test_v2_is_v1_plus_utm_campaign() -> None:
    assert _names(2) == [*_names(1), "utm_campaign"]


def test_every_event_field_is_in_v2() -> None:
    assert {f.name for f in fields(Event)} == set(_names(2))


def test_v1_and_v2_roundtrip_a_null_heavy_record() -> None:
    for version in (1, 2):
        record: dict[str, object] = dict.fromkeys(_names(version))
        record.update(event_type="page_view", device="mobile", event_ts="2017-06-01T12:00:00")
        schema = fastavro.parse_schema(_load(version))
        buffer = io.BytesIO()
        fastavro.schemaless_writer(buffer, schema, record)
        buffer.seek(0)
        decoded = fastavro.schemaless_reader(buffer, schema, schema)
        assert decoded["event_type"] == "page_view"  # type: ignore[call-overload,index]
