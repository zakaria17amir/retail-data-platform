from datetime import date
from pathlib import Path
from urllib.parse import urlparse

import pytest
from lakehouse_spark.cloud import entrypoint
from lakehouse_spark.cloud.export import export_silver
from pyspark.sql import DataFrame, SparkSession

pytestmark = pytest.mark.spark

ORDERS = [("o1", "delivered", False), ("o2", "canceled", True), ("o3", "shipped", False)]
CUSTOMERS = [
    ("c1", "SP", date(2017, 1, 1), date(2017, 6, 1), False),
    ("c1", "RJ", date(2017, 6, 1), None, True),
    ("c2", "MG", date(2017, 1, 1), None, True),
]


def _silver(spark: SparkSession, root: str) -> dict[str, DataFrame]:
    tables = {
        "sales/orders": spark.createDataFrame(
            ORDERS, "order_id string, order_status string, _is_deleted boolean"
        ),
        "party/customers": spark.createDataFrame(
            CUSTOMERS,
            "customer_id string, customer_state string, valid_from date, valid_to date, "
            "is_current boolean",
        ),
    }
    for name, df in tables.items():
        df.write.format("delta").save(f"{root}/silver/{name}")
    return tables


def _folder(root: str, table: str, run_id: str) -> Path:
    return Path(urlparse(root).path) / "export" / "silver" / table / run_id


def _visible(folder: Path) -> list[str]:
    return sorted(p.name for p in folder.iterdir() if not p.name.startswith("."))


def test_export_writes_every_row_as_parquet_with_success_marker(
    spark: SparkSession, root: str
) -> None:
    tables = _silver(spark, root)
    counts = export_silver(spark, root, "r1", ("sales/orders", "party/customers"))
    assert counts == {"sales/orders": 3, "party/customers": 3}
    for name, expected in tables.items():
        leaf = name.rsplit("/", 1)[-1]
        folder = _folder(root, leaf, "r1")
        files = _visible(folder)
        assert files[0] == "_SUCCESS"
        parts = files[1:]
        assert parts and all(f.startswith("part-") and f.endswith(".parquet") for f in parts)
        exported = spark.read.parquet(str(folder))
        assert exported.schema == expected.schema
        assert exported.exceptAll(expected).count() == 0
        assert expected.exceptAll(exported).count() == 0


def test_export_rerun_overwrites_only_its_run_id_with_stable_file_names(
    spark: SparkSession, root: str
) -> None:
    _silver(spark, root)
    export_silver(spark, root, "r1", ("sales/orders",))
    export_silver(spark, root, "r2", ("sales/orders",))
    before = _visible(_folder(root, "orders", "r1"))
    export_silver(spark, root, "r1", ("sales/orders",))
    assert _visible(_folder(root, "orders", "r1")) == before
    assert spark.read.parquet(str(_folder(root, "orders", "r1"))).count() == len(ORDERS)
    assert spark.read.parquet(str(_folder(root, "orders", "r2"))).count() == len(ORDERS)
    staging = Path(urlparse(root).path) / "export" / "_staging"
    assert not [p for p in staging.rglob("*") if p.is_file()]


def test_export_skips_missing_tables(spark: SparkSession, root: str) -> None:
    assert export_silver(spark, root, "r1", ("events/clickstream",)) == {}
    assert not _folder(root, "clickstream", "r1").exists()


def test_entrypoint_export_exports_all_silver_tables_present(
    spark: SparkSession, root: str
) -> None:
    _silver(spark, root)
    assert entrypoint.main(["export", "--run-id", "r1", "--root", root]) == 0
    assert _visible(_folder(root, "customers", "r1"))[0] == "_SUCCESS"
    assert _visible(_folder(root, "orders", "r1"))[0] == "_SUCCESS"


def test_entrypoint_silver_runs_every_table_with_the_given_run_id(
    spark: SparkSession, root: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from lakehouse_spark.silver import job
    from lakehouse_spark.silver.tables import TABLES

    calls: list[tuple[str, str, str]] = []
    monkeypatch.setattr(
        job, "run_table", lambda _spark, spec, r, run_id: calls.append((spec.name, r, run_id))
    )
    assert entrypoint.main(["silver", "--run-id", "r9", "--root", root]) == 0
    assert calls == [(spec.name, root, "r9") for spec in TABLES]
