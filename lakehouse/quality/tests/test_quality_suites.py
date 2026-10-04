import shutil
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

import pandas as pd
import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake
from lakehouse_quality.io import bronze_distinct_keys, read_silver, rejected_distinct_keys
from lakehouse_quality.run import main
from lakehouse_quality.suites import TABLES, Check, gx_errors, run_checks

LOADED = datetime(2026, 1, 1, 12, tzinfo=UTC)
NOW = LOADED + timedelta(hours=1)


def _silver(
    root: str,
    table: str,
    rows: list[dict[str, Any]],
    mode: Literal["overwrite", "append"] = "overwrite",
) -> None:
    spec = TABLES[table]
    meta: dict[str, Any] = {"_silver_loaded_at": LOADED, "_run_id": "r1"}
    if spec.bronze:
        meta |= {"_source_lsn": 1, "_source_ts": LOADED, "_is_deleted": False}
    if spec.scd2:
        meta |= {"valid_from": LOADED, "valid_to": None, "is_current": True}
    filled = [{c: "x" for c in spec.columns} | meta | row for row in rows]
    frame = pd.DataFrame(filled)
    if spec.scd2:
        frame["valid_to"] = pd.to_datetime(frame["valid_to"], utc=True)
    data = pa.Table.from_pandas(frame, preserve_index=False)
    write_deltalake(f"{root}/silver/{table}", data, mode=mode)


def _bronze(root: str, table: str, key: list[str], afters: list[Any], befores: list[Any]) -> None:
    struct = pa.struct([(k, pa.string()) for k in key])
    data = pa.table(
        {
            "op": ["c" if a else "d" for a in afters],
            "after": pa.array(afters, type=struct),
            "before": pa.array(befores, type=struct),
        }
    )
    write_deltalake(f"{root}/bronze/olist/{table}", data, mode="overwrite", schema_mode="overwrite")


def _cdc(root: str, silver: str, bronze: str, key: list[str], rows: list[dict[str, Any]]) -> None:
    _silver(root, silver, rows)
    keys = [{k: str(r[k]) for k in key} for r in rows]
    _bronze(root, bronze, key, keys, [None] * len(keys))


@pytest.fixture
def lake(tmp_path: Path) -> str:
    root = tmp_path.as_posix()
    _cdc(
        root,
        "catalog/categories",
        "product_category_name_translation",
        ["product_category_name"],
        [{"product_category_name": "cat"}],
    )
    _silver(
        root,
        "catalog/products",
        [
            {"product_id": "p1", "is_current": False, "valid_to": LOADED},
            {"product_id": "p1"},
            {"product_id": "p2"},
        ],
    )
    _bronze(
        root, "products", ["product_id"], [{"product_id": "p1"}, {"product_id": "p2"}], [None, None]
    )
    _cdc(root, "party/customers", "customers", ["customer_id"], [{"customer_id": "c1"}])
    _cdc(root, "party/sellers", "sellers", ["seller_id"], [{"seller_id": "s1"}])
    _cdc(
        root,
        "geo/geolocation_points",
        "geolocation",
        ["geolocation_pk"],
        [{"geolocation_pk": "1"}, {"geolocation_pk": "2"}],
    )
    _silver(root, "geo/zip_centroids", [{"zip_code_prefix": "01001"}])
    _cdc(
        root,
        "sales/orders",
        "orders",
        ["order_id"],
        [
            {
                "order_id": "o1",
                "customer_id": "c1",
                "order_purchase_ts_utc": datetime(2018, 1, 1, tzinfo=UTC),
            },
            {
                "order_id": "o2",
                "customer_id": "c1",
                "order_purchase_ts_utc": datetime(2018, 3, 1, tzinfo=UTC),
            },
        ],
    )
    items = [
        {
            "order_id": o,
            "order_item_id": "1",
            "product_id": "p1",
            "seller_id": "s1",
            "price": Decimal("10.00"),
            "freight_value": Decimal("5.00"),
        }
        for o in ("o1", "o2")
    ]
    _cdc(root, "sales/order_items", "order_items", ["order_id", "order_item_id"], items)
    _silver(
        root,
        "sales/order_payments",
        [
            {"order_id": "o1", "payment_sequential": "1"},
            {"order_id": "o2", "payment_sequential": "1", "_is_deleted": True},
        ],
    )
    # the deleted payment only appears in `before`, as Debezium writes deletes
    _bronze(
        root,
        "order_payments",
        ["order_id", "payment_sequential"],
        [{"order_id": "o1", "payment_sequential": "1"}, None],
        [None, {"order_id": "o2", "payment_sequential": "1"}],
    )
    _cdc(
        root,
        "sales/order_reviews",
        "order_reviews",
        ["review_pk"],
        [{"review_pk": "1", "order_id": "o1", "review_score": 5}],
    )
    _silver(
        root,
        "events/clickstream",
        [
            {"event_id": "e1", "product_id": "p1"},
            {"event_id": "e2", "product_id": None},
        ],
    )
    return root


def _failed(checks: list[Check]) -> list[Check]:
    return [c for c in checks if not c.success]


def test_io_reads_current_rows_and_bronze_keys(lake: str) -> None:
    assert sorted(read_silver(lake, "catalog/products")["product_id"]) == ["p1", "p2"]
    assert len(read_silver(lake, "catalog/products", current_only=False)) == 3
    assert list(read_silver(lake, "sales/order_payments")["order_id"]) == ["o1"]
    assert bronze_distinct_keys(lake, "order_payments", ["order_id", "payment_sequential"]) == 2
    _bronze(
        lake, "sellers", ["seller_id"], [None, None], [{"seller_id": "s1"}, {"seller_id": "s2"}]
    )
    assert bronze_distinct_keys(lake, "sellers", ["seller_id"]) == 2


def test_clean_tables_pass_all(lake: str) -> None:
    checks = run_checks(lake, NOW)
    assert _failed(checks) == []
    assert {c.table for c in checks} == set(TABLES)
    names = {c.expectation for c in checks}
    assert "rowcount_vs_bronze" in names
    assert "expect_column_values_to_be_between(freight_value)" in names
    assert "referential_integrity(seller_id -> party/sellers)" in names


def test_duplicate_key_is_critical(lake: str) -> None:
    _silver(
        lake,
        "sales/orders",
        [
            {
                "order_id": "o1",
                "customer_id": "c1",
                "order_purchase_ts_utc": datetime(2018, 1, 1, tzinfo=UTC),
            }
        ],
        mode="append",
    )
    failed = _failed(run_checks(lake, NOW))
    assert [(c.table, c.expectation, c.severity) for c in failed] == [
        ("sales/orders", "expect_column_values_to_be_unique(order_id)", "critical")
    ]


def test_orphan_order_item_is_critical(lake: str) -> None:
    _silver(
        lake,
        "sales/order_items",
        [
            {
                "order_id": "o9",
                "order_item_id": "1",
                "product_id": "p1",
                "seller_id": "s1",
                "price": Decimal("1.00"),
                "freight_value": Decimal("1.00"),
            }
        ],
        mode="append",
    )
    failed = {(c.table, c.expectation, c.severity): c for c in _failed(run_checks(lake, NOW))}
    orphan = failed[
        ("sales/order_items", "referential_integrity(order_id -> sales/orders)", "critical")
    ]
    assert orphan.observed == "orphans=1"


def test_rowcount_gap_is_critical(lake: str) -> None:
    _bronze(
        lake,
        "orders",
        ["order_id"],
        [{"order_id": "o1"}, {"order_id": "o2"}, {"order_id": "o3"}],
        [None, None, None],
    )
    failed = _failed(run_checks(lake, NOW))
    assert [(c.table, c.expectation, c.severity) for c in failed] == [
        ("sales/orders", "rowcount_vs_bronze", "critical")
    ]
    write_deltalake(
        f"{lake}/silver/_rejects/sales/orders",
        pa.table(
            {
                "rule_id": ["orders_null_purchase_ts"],
                "reason": ["null"],
                "record_json": ['{"order_id": "o3"}'],
            }
        ),
    )
    assert _failed(run_checks(lake, NOW)) == []


def test_bronze_keys_skip_null_and_reject_rows_without_key(lake: str) -> None:
    _bronze(lake, "orders", ["order_id"], [{"order_id": "o1"}, {"order_id": None}], [None, None])
    assert bronze_distinct_keys(lake, "orders", ["order_id"]) == 1
    write_deltalake(
        f"{lake}/silver/_rejects/sales/orders",
        pa.table({"record_json": ['{"order_id": "o3"}', '{"other": 1}', '{"order_id": null}']}),
    )
    assert rejected_distinct_keys(lake, "sales/orders", ["order_id"]) == 1


def test_non_positive_price_is_critical(lake: str) -> None:
    _silver(
        lake,
        "sales/order_items",
        [
            {
                "order_id": o,
                "order_item_id": "1",
                "product_id": "p1",
                "seller_id": "s1",
                "price": Decimal(price),
                "freight_value": Decimal("5.00"),
            }
            for o, price in (("o1", "0.00"), ("o2", "10.00"))
        ],
    )
    failed = _failed(run_checks(lake, NOW))
    assert [(c.table, c.expectation, c.severity) for c in failed] == [
        ("sales/order_items", "expect_column_values_to_be_between(price)", "critical")
    ]


def test_missing_silver_table_is_critical(lake: str) -> None:
    shutil.rmtree(f"{lake}/silver/geo/zip_centroids")
    failed = _failed(run_checks(lake, NOW))
    assert [(c.table, c.expectation, c.severity) for c in failed] == [
        ("geo/zip_centroids", "table_exists", "critical")
    ]


def test_missing_key_column_surfaces_gx_message(lake: str) -> None:
    rows = read_silver(lake, "sales/orders", current_only=False).drop(columns="order_id")
    write_deltalake(f"{lake}/silver/sales/orders", rows, mode="overwrite", schema_mode="overwrite")
    failed = {c.expectation: c for c in _failed(run_checks(lake, NOW)) if c.table == "sales/orders"}
    not_null = failed["expect_column_values_to_not_be_null(order_id)"]
    assert not_null.severity == "critical"
    assert 'The column "order_id" in BatchData does not exist' in not_null.details
    assert failed["rowcount_vs_bronze"].observed == "error"


def test_gx_errors_handles_flat_and_nested_exception_info() -> None:
    flat = {"raised_exception": True, "exception_message": "boom", "exception_traceback": "tb"}
    nested = {"MetricConfigurationID(x)": flat}
    ok = {"raised_exception": False, "exception_message": None, "exception_traceback": None}
    assert gx_errors(flat) == ["boom"]
    assert gx_errors(nested) == ["boom"]
    assert gx_errors(ok) == []


def test_bronze_read_error_is_recorded_not_raised(lake: str) -> None:
    _bronze(lake, "orders", ["id"], [{"id": "o1"}], [None])
    failed = {(c.table, c.expectation): c for c in _failed(run_checks(lake, NOW))}
    rowcount = failed[("sales/orders", "rowcount_vs_bronze")]
    assert (rowcount.severity, rowcount.observed) == ("critical", "error")
    assert "order_id" in rowcount.details
    assert main(["run", "--root", lake]) == 1
    assert len(DeltaTable(f"{lake}/silver/_dq_results").to_pandas()) > 0


def test_stale_table_is_warning(lake: str) -> None:
    failed = _failed(run_checks(lake, NOW + timedelta(hours=2)))
    assert failed
    assert {(c.expectation, c.severity) for c in failed} == {("freshness", "warning")}
    assert "events/clickstream" in {c.table for c in failed}


def test_main_exit_codes_and_dq_results_appended(
    lake: str, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["run", "--root", lake]) == 0
    first = len(capsys.readouterr().out.splitlines())
    results = DeltaTable(f"{lake}/silver/_dq_results").to_pandas()
    assert len(results) == first
    assert set(results["severity"][~results["success"]]) == {"warning"}

    _silver(lake, "party/customers", [{"customer_id": "c1"}], mode="append")
    assert main(["run", "--root", lake]) == 1
    results = DeltaTable(f"{lake}/silver/_dq_results").to_pandas()
    assert len(results) == 2 * first
    assert results["run_id"].nunique() == 2
    assert list(results.columns) == [
        "run_id",
        "table",
        "expectation",
        "severity",
        "success",
        "observed_value",
        "details",
        "checked_at",
    ]


def _empty(path: str, columns: dict[str, pa.DataType]) -> None:
    schema = pa.schema(list(columns.items()))
    write_deltalake(path, schema.empty_table(), mode="overwrite", schema_mode="overwrite")


def test_empty_tables_do_not_fail_criticals(lake: str, capsys: pytest.CaptureFixture[str]) -> None:
    ts = pa.timestamp("us", tz="UTC")
    events = {c: pa.string() for c in TABLES["events/clickstream"].columns}
    _empty(f"{lake}/silver/events/clickstream", events | {"_silver_loaded_at": ts})
    categories = {c: pa.string() for c in TABLES["catalog/categories"].columns}
    _empty(
        f"{lake}/silver/catalog/categories",
        categories | {"_silver_loaded_at": ts, "_is_deleted": pa.bool_()},
    )
    row = pa.struct([("product_category_name", pa.string())])
    _empty(
        f"{lake}/bronze/olist/product_category_name_translation",
        {"op": pa.string(), "after": row, "before": row},
    )

    checks = run_checks(lake, NOW)
    failed = _failed(checks)
    assert [(c.table, c.expectation, c.severity, c.observed) for c in failed] == [
        ("catalog/categories", "freshness", "warning", "empty"),
        ("events/clickstream", "freshness", "warning", "empty"),
    ]
    rowcount = next(
        c
        for c in checks
        if (c.table, c.expectation) == ("catalog/categories", "rowcount_vs_bronze")
    )
    assert rowcount.observed == "silver=0 bronze=0 rejected=0"
    assert any(c.expectation == "product_id_in_products" and c.success for c in checks)
    assert main(["run", "--root", lake]) == 0
