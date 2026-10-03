from pathlib import Path

import polars as pl
import pytest
from replayer.cli import main
from replayer.sample import sample
from replayer.schema import TABLES


def _read(directory: Path, name: str) -> pl.DataFrame:
    table = next(t for t in TABLES if t.name == name)
    return pl.read_csv(directory / table.csv_file, infer_schema=False)


def test_sample_is_referentially_consistent(tmp_csv_dir: Path, tmp_path: Path) -> None:
    dest = tmp_path / "out"
    counts = sample(tmp_csv_dir, dest, n_orders=1)
    assert counts["orders"] == 1
    order_ids = set(_read(dest, "orders")["order_id"])
    assert len(order_ids) == 1
    customers = set(_read(dest, "customers")["customer_id"])
    products = set(_read(dest, "products")["product_id"])
    sellers = set(_read(dest, "sellers")["seller_id"])
    assert set(_read(dest, "orders")["customer_id"]) <= customers
    items = _read(dest, "order_items")
    assert set(items["product_id"]) <= products
    assert set(items["seller_id"]) <= sellers
    for name in ("order_items", "order_payments", "order_reviews"):
        assert set(_read(dest, name)["order_id"]) <= order_ids
    categories = set(_read(dest, "product_category_name_translation")["product_category_name"])
    assert categories == {c for c in _read(dest, "products")["product_category_name"] if c}


def test_sample_is_deterministic(tmp_csv_dir: Path, tmp_path: Path) -> None:
    first, second = tmp_path / "a", tmp_path / "b"
    assert sample(tmp_csv_dir, first, n_orders=1, seed=7) == sample(
        tmp_csv_dir, second, n_orders=1, seed=7
    )
    for t in TABLES:
        assert (first / t.csv_file).read_bytes() == (second / t.csv_file).read_bytes()


def test_sample_takes_all_orders_when_n_is_large(tmp_csv_dir: Path, tmp_path: Path) -> None:
    assert sample(tmp_csv_dir, tmp_path / "out", n_orders=100)["orders"] == 2


def test_sample_cli_prints_counts(
    tmp_csv_dir: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    dest = tmp_path / "out"
    argv = ["sample", "--src", str(tmp_csv_dir), "--dest", str(dest), "--n-orders", "1"]
    assert main(argv) == 0
    assert "orders: 1" in capsys.readouterr().out
