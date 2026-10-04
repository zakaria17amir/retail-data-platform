import json
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

import pytest

pytest.importorskip("deltalake")
pytest.importorskip("pyarrow")

import pyarrow as pa  # noqa: E402
from deltalake import write_deltalake  # noqa: E402
from lakehouse_spark.cli import main  # noqa: E402


def _write(path: Path, **columns: Sequence[object]) -> None:
    write_deltalake(str(path), pa.table(columns))


def _status(root: Path, capsys: pytest.CaptureFixture[str], *flags: str) -> str:
    assert main(["status", "--root", root.as_uri(), *flags]) == 0
    return capsys.readouterr().out


def test_status_json_lists_tables_and_zero_for_missing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _write(tmp_path / "bronze" / "olist" / "orders", order_id=["a", "b", "c"])
    counts = json.loads(_status(tmp_path, capsys, "--json"))
    assert counts["bronze/olist/orders"] == 3
    assert counts["bronze/olist/customers"] == 0
    assert counts["bronze/events/page_view"] == 0
    assert counts["quarantine/events"] == 0
    assert not any("reasons" in key for key in counts)


def test_status_sums_records_across_add_actions(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "bronze" / "olist" / "orders"
    _write(path, order_id=["a", "b", "c"])
    write_deltalake(str(path), pa.table({"order_id": ["d"]}), mode="append")
    assert json.loads(_status(tmp_path, capsys, "--json"))["bronze/olist/orders"] == 4


def test_status_text_is_sorted_one_per_line(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _write(tmp_path / "bronze" / "olist" / "orders", order_id=["a"])
    lines = _status(tmp_path, capsys).splitlines()
    assert lines == sorted(lines)
    assert "bronze/olist/orders: 1" in lines
    assert "bronze/olist/sellers: 0" in lines


def test_quarantine_reason_histogram(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    reasons = ["null_primary_key", "unparseable_timestamp", "null_primary_key"]
    _write(tmp_path / "bronze" / "_quarantine" / "events", reason=reasons)
    out = _status(tmp_path, capsys)
    assert "quarantine/events: 3" in out
    assert "quarantine/events reasons: " in out
    histogram = json.loads(_status(tmp_path, capsys, "--json"))["quarantine/events reasons"]
    assert histogram == {"null_primary_key": 2, "unparseable_timestamp": 1}


def test_status_lists_silver_rejects_and_latest_rule_metrics(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    silver = tmp_path / "silver"
    _write(
        silver / "catalog" / "products", product_id=["a", "a", "b"], is_current=[False, True, True]
    )
    _write(silver / "sales" / "orders", order_id=["o1", "o2"])
    _write(silver / "_rejects" / "geo" / "geolocation_points", rule_id=["geo_out_of_bbox"] * 2)
    geo, events = "geo/geolocation_points", "events/clickstream"
    _write(
        silver / "_rule_metrics",
        run_id=["r0", "r1", "r1", "r1", "r1"],
        table=[geo, geo, geo, geo, events],
        rule_id=[
            "geo_out_of_bbox",
            "geo_out_of_bbox",
            "geo_out_of_bbox",
            "rename_columns",
            "event_duplicate",
        ],
        rows_in=[10, 600_000, 400_000, 1_000_000, 200],
        rows_rejected=[5, 15, 12, 0, 1],
        pct_rejected=[50.0, 0.0025, 0.003, 0.0, 0.5],
        run_ts=[
            datetime(2017, 1, 1),
            datetime(2017, 1, 2),
            datetime(2017, 1, 2),
            datetime(2017, 1, 2),
            datetime(2017, 1, 2, 1),
        ],
    )
    lines = _status(tmp_path, capsys).splitlines()
    assert "silver/catalog/products: 2" in lines
    assert "silver/sales/orders: 2" in lines
    assert "silver/party/customers: 0" in lines
    assert "silver/_rejects/geo/geolocation_points: 2" in lines
    assert [line for line in lines if line.startswith("rule ")] == [
        "rule event_duplicate rejected 1 rows (0.500 %) in events/clickstream",
        "rule geo_out_of_bbox rejected 27 rows (0.003 %) in geo/geolocation_points",
    ]
