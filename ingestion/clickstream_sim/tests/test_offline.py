import random
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest
from clickstream_sim.cli import main
from clickstream_sim.events import TS_FORMAT, Catalogue, OrderRef, order_sessions
from clickstream_sim.offline import catalogue_from_products, generate, history, orders_from_gold

CATALOGUE = Catalogue.from_rows([("p1", "toys"), ("p2", "toys"), ("p3", "books"), ("p4", None)])


def _training() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "order_id": ["o2", "o1", "o3"],
            "customer_id": ["c2", "c1", "c3"],
            "order_purchase_ts_utc": [
                datetime(2017, 10, 2, 11, tzinfo=UTC),
                datetime(2017, 10, 2, 9, 30, 15, 500_000, tzinfo=UTC),
                datetime(2017, 10, 3, tzinfo=UTC),
            ],
            "is_late": [False, True, False],
        }
    )


def _items() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "order_id": ["o1", "o1", "o1", "o2", "x9"],
            "order_item_id": [2, 1, 3, 1, 1],
            "product_id": ["p3", "p1", "p3", "p2", "p4"],
        }
    )


def _products() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "product_id": ["p1", "p2", "p3", "p4", "p1"],
            "product_category_name": ["brinquedos", "brinquedos", "livros", None, "old"],
            "product_category_name_english": ["toys", "toys", "books", "unknown", "old"],
            "is_current": [True, True, True, True, False],
        }
    )


def _orders() -> list[OrderRef]:
    return [
        OrderRef("o1", "c1", datetime(2017, 10, 2, 9), ("p1", "p3")),
        OrderRef("o2", "c2", datetime(2017, 10, 2, 11), ("p2",)),
        OrderRef("o3", "c3", datetime(2017, 10, 3, 9), ("p1", "p2", "p4")),
    ]


def test_orders_from_gold_joins_items_in_item_order_and_skips_orders_without_items() -> None:
    orders, skipped = orders_from_gold(_training(), _items())
    assert orders == [
        OrderRef("o1", "c1", datetime(2017, 10, 2, 9, 30, 15, 500_000), ("p1", "p3")),
        OrderRef("o2", "c2", datetime(2017, 10, 2, 11), ("p2",)),
    ]
    assert skipped == 1


def test_catalogue_from_products_uses_current_rows_like_the_live_catalogue() -> None:
    catalogue = catalogue_from_products(_products())
    assert catalogue == Catalogue.from_rows(
        [("p1", "brinquedos"), ("p2", "brinquedos"), ("p3", "livros"), ("p4", None)]
    )


def test_history_is_deterministic_for_a_seed() -> None:
    first = history(_orders(), CATALOGUE, seed=42, browsing_ratio=3)
    assert first.equals(history(_orders(), CATALOGUE, seed=42, browsing_ratio=3))
    assert not first.equals(history(_orders(), CATALOGUE, seed=43, browsing_ratio=3))


def test_history_row_counts_per_session_type() -> None:
    frame = history(_orders(), CATALOGUE, seed=7, browsing_ratio=2)
    sessions = frame.group_by("session_id").agg(
        pl.len().alias("rows"),
        (pl.col("event_type") == "checkout_started").any().alias("converting"),
        pl.col("customer_id").is_not_null().all().alias("has_customer"),
    )
    converting = sessions.filter("converting")
    browsing = sessions.filter(~pl.col("converting"))
    assert converting.height == 3
    assert browsing.height == 6
    assert converting["has_customer"].all()
    assert not browsing["has_customer"].any()

    rng = random.Random(7)
    expected = [order_sessions(o, CATALOGUE, rng, 2) for o in _orders()]
    assert converting["rows"].sum() == sum(len(s[0]) for s in expected)
    assert browsing["rows"].sum() == sum(len(b) for s in expected for b in s[1:])
    assert frame.height == sum(len(session) for s in expected for session in s)


def test_is_purchase_target_marks_purchased_products_in_converting_sessions_only() -> None:
    frame = history(_orders(), CATALOGUE, seed=7, browsing_ratio=2)
    targets = frame.filter("is_purchase_target")
    assert targets["customer_id"].is_not_null().all()
    for order in _orders():
        mine = targets.filter(pl.col("customer_id") == order.customer_id)
        assert set(mine["product_id"]) == set(order.product_ids)
    carts = frame.filter(
        (pl.col("event_type") == "add_to_cart") & pl.col("customer_id").is_not_null()
    )
    assert carts["is_purchase_target"].all()
    assert not frame.filter(pl.col("event_type") == "checkout_started")["is_purchase_target"].any()


def test_history_columns_are_the_event_fields_plus_target_in_dataset_time() -> None:
    frame = history(_orders(), CATALOGUE, seed=1, browsing_ratio=1)
    assert frame.columns[-1] == "is_purchase_target"
    assert "rank" in frame.columns and "utm_campaign" in frame.columns
    stamps = [datetime.strptime(ts, TS_FORMAT) for ts in frame["event_ts"]]
    assert min(stamps) >= datetime(2017, 10, 2, 7)
    assert max(stamps) <= datetime(2017, 10, 3, 11)
    assert frame["event_id"].n_unique() == frame.height


@pytest.fixture
def gold(tmp_path: Path) -> Path:
    (tmp_path / "ml").mkdir()
    _training().write_parquet(tmp_path / "ml" / "late_delivery_training.parquet")
    _products().write_parquet(tmp_path / "dim_product.parquet")
    _items().write_parquet(tmp_path / "fct_order_items.parquet")
    return tmp_path


def test_generate_writes_parquet_and_reports_counts(gold: Path, tmp_path: Path) -> None:
    out = tmp_path / "out" / "sessions_offline.parquet"
    summary = generate(gold, out, seed=42, browsing_ratio=3)
    frame = pl.read_parquet(out)
    assert summary["rows"] == frame.height
    assert summary["orders"] == 2
    assert summary["orders_without_items"] == 1
    assert (summary["sessions_converting"], summary["sessions_browsing"]) == (2, 6)
    assert summary["rows_converting"] + summary["rows_browsing"] == frame.height
    assert summary["purchase_targets"] == frame["is_purchase_target"].sum()


def test_generate_cli(
    gold: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("SIM_SEED", "5")
    monkeypatch.setenv("SIM_BROWSING_RATIO", "1")
    out = tmp_path / "s.parquet"
    assert main(["generate", "--gold-dir", str(gold), "--out", str(out)]) == 0
    assert "sessions_browsing: 2" in capsys.readouterr().out
    assert pl.read_parquet(out).equals(
        history(
            orders_from_gold(_training(), _items())[0],
            catalogue_from_products(_products()),
            seed=5,
            browsing_ratio=1,
        )
    )
