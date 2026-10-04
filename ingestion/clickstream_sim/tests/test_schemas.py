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
    for version in (1, 2, 3):
        fastavro.parse_schema(_load(version))


def test_v2_is_v1_plus_utm_campaign() -> None:
    assert _names(2) == [*_names(1), "utm_campaign"]


def test_v3_is_v2_plus_nullable_recommendation_fields() -> None:
    assert _names(3) == [*_names(2), "rank", "rec_model_version", "rec_strategy"]
    added = {f["name"]: f for f in _load(3)["fields"]}  # type: ignore[attr-defined]
    assert added["rank"]["type"] == ["null", "int"]
    for name in ("rank", "rec_model_version", "rec_strategy"):
        assert added[name]["default"] is None


def test_every_event_field_is_in_v3() -> None:
    assert [f.name for f in fields(Event)] == _names(3)


def test_null_heavy_record_roundtrips_in_every_version() -> None:
    for version in (1, 2, 3):
        record: dict[str, object] = dict.fromkeys(_names(version))
        record.update(event_type="page_view", device="mobile", event_ts="2017-06-01T12:00:00")
        schema = fastavro.parse_schema(_load(version))
        buffer = io.BytesIO()
        fastavro.schemaless_writer(buffer, schema, record)
        buffer.seek(0)
        decoded = fastavro.schemaless_reader(buffer, schema, schema)
        assert decoded["event_type"] == "page_view"  # type: ignore[call-overload,index]


def test_v3_reader_reads_v2_data_with_null_recommendation_fields() -> None:
    record: dict[str, object] = dict.fromkeys(_names(2))
    record.update(event_type="page_view", device="mobile", event_ts="2017-06-01T12:00:00")
    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, fastavro.parse_schema(_load(2)), record)
    buffer.seek(0)
    decoded = fastavro.schemaless_reader(
        buffer, fastavro.parse_schema(_load(2)), fastavro.parse_schema(_load(3))
    )
    assert decoded["rank"] is None  # type: ignore[call-overload,index]
