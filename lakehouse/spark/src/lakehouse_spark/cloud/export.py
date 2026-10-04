"""Silver Delta -> plain Parquet for Snowpipe, which can't follow a Delta log.

Each table's full current snapshot - what dbt's delta_scan reads locally: current-state tables with
their soft-deleted rows (_is_deleted), SCD2 tables with all versions (is_current) - lands in
<root>/export/silver/<leaf table name>/<run_id>/ as part-NNNNN.parquet plus a final _SUCCESS.

Spark writes to <root>/export/_staging/ (outside the Snowpipe prefix) and the parts are then moved
in under stable names, so re-running a run_id replaces only that folder and reuses the same paths:
Snowpipe's load history skips paths it has already loaded, so a retried export is not loaded twice.
A new run_id is a new full snapshot.
"""

import logging
from collections.abc import Iterable
from typing import Any

from pyspark.sql import SparkSession

from lakehouse_spark.cli import SILVER_TABLES
from lakehouse_spark.silver.tables import read_if_exists

log = logging.getLogger("export")


def _publish(spark: SparkSession, staging: str, final: str) -> None:
    jvm: Any = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path
    src, dst = path(staging), path(final)
    fs = src.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    fs.delete(dst, True)
    fs.mkdirs(dst)
    parts = sorted(
        s.getPath().getName()
        for s in fs.listStatus(src)
        if s.getPath().getName().startswith("part-")
    )
    for i, part in enumerate(parts):
        if not fs.rename(path(src, part), path(dst, f"part-{i:05d}.parquet")):
            raise OSError(f"could not move {staging}/{part} to {final}")
    fs.create(path(dst, "_SUCCESS"), True).close()
    fs.delete(src, True)


def export_table(spark: SparkSession, root: str, table: str, run_id: str) -> int | None:
    df = read_if_exists(spark, f"{root}/silver/{table}")
    if df is None:
        log.info("%s: no silver table, skipped", table)
        return None
    name = table.rsplit("/", 1)[-1]
    staging = f"{root}/export/_staging/{name}/{run_id}"
    df.write.mode("overwrite").parquet(staging)
    _publish(spark, staging, f"{root}/export/silver/{name}/{run_id}")
    rows = df.count()
    log.info("%s: exported %d rows (run %s)", table, rows, run_id)
    return rows


def export_silver(
    spark: SparkSession, root: str, run_id: str, tables: Iterable[str] = SILVER_TABLES
) -> dict[str, int]:
    counts = {t: export_table(spark, root, t, run_id) for t in tables}
    return {t: n for t, n in counts.items() if n is not None}
