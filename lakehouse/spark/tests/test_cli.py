import json
from collections.abc import Sequence
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
