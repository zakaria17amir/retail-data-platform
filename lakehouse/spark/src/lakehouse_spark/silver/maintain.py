import argparse
import logging
import os

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from lakehouse_spark.bronze.app import build_session
from lakehouse_spark.cli import EVENT_TYPES, OLIST_TABLES, QUARANTINE_SOURCES
from lakehouse_spark.silver.tables import TABLES

ZORDER = {
    "sales/orders": ("customer_id",),
    "sales/order_items": ("product_id", "seller_id"),
    "events/clickstream": ("session_id",),
}
log = logging.getLogger("maintain")


def table_paths() -> list[str]:
    names = [spec.name for spec in TABLES]
    return [
        *(f"bronze/olist/{t}" for t in OLIST_TABLES),
        *(f"bronze/events/{t}" for t in EVENT_TYPES),
        *(f"bronze/_quarantine/{s}" for s in QUARANTINE_SOURCES),
        *(f"silver/{n}" for n in [*names, "geo/zip_centroids"]),
        *(f"silver/_rejects/{n}" for n in names),
        "silver/_rule_metrics",
        "silver/_dq_results",
    ]


def _num_files(table: DeltaTable) -> int:
    return int(table.detail().collect()[0]["numFiles"])


def optimize_and_vacuum(spark: SparkSession, root: str, retain_hours: int = 168) -> dict[str, int]:
    after: dict[str, int] = {}
    for rel in table_paths():
        path = f"{root}/{rel}"
        if not DeltaTable.isDeltaTable(spark, path):
            continue
        table = DeltaTable.forPath(spark, path)
        before = _num_files(table)
        zorder = ZORDER.get(rel.removeprefix("silver/")) if rel.startswith("silver/") else None
        if zorder:
            table.optimize().executeZOrderBy(*zorder)
        else:
            table.optimize().executeCompaction()
        table.vacuum(retain_hours)
        after[path] = _num_files(table)
        log.info("%s: %d files -> %d", rel, before, after[path])
    return after


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="maintain")
    parser.add_argument(
        "--root", default=f"s3a://{os.environ.get('LAKEHOUSE_BUCKET', 'lakehouse')}"
    )
    parser.add_argument("--retain-hours", type=int, default=168)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    spark = build_session("maintain")
    # VACUUM lists files with this parallelism (default 10000 tasks, ~26 s per table on dev)
    spark.conf.set("spark.sql.sources.parallelPartitionDiscovery.parallelism", "8")
    optimize_and_vacuum(spark, args.root.rstrip("/"), args.retain_hours)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
