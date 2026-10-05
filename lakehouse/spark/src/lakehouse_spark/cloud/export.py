"""Silver Delta -> plain Parquet for Snowpipe, which can't follow a Delta log.

Each table's full current snapshot - what dbt's delta_scan reads locally: current-state tables with
their soft-deleted rows (_is_deleted), SCD2 tables with all versions (is_current), plus the
_rule_metrics / _dq_results history - lands in <root>/export/silver/<silver table>/<run_id>/
(mirroring silver/, e.g. catalog/categories) as part-NNNNN.parquet plus a final _SUCCESS.
Timestamps are written as INT64 TIMESTAMP_MICROS (UTC-adjusted), not Spark's default INT96, which
Snowflake can't map to TIMESTAMP_LTZ.

Spark writes to <root>/export/_staging/ (outside the Snowpipe prefix) and the parts are then moved
in under stable names, so re-running a run_id replaces only that folder and reuses the same paths:
Snowpipe's load history skips paths it has already loaded, so a retried export is not loaded twice.
A new run_id is a new full snapshot; dbt on snowflake reads only the latest run (latest_export).
"""

import logging
from collections.abc import Iterable
from typing import Any

from pyspark.sql import SparkSession

from lakehouse_spark.cli import SILVER_TABLES
from lakehouse_spark.silver.tables import read_if_exists

EXPORT_TABLES = (*SILVER_TABLES, "_rule_metrics", "_dq_results")
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
    staging = f"{root}/export/_staging/{table}/{run_id}"
    df.write.mode("overwrite").parquet(staging)
    _publish(spark, staging, f"{root}/export/silver/{table}/{run_id}")
    rows = df.count()
    log.info("%s: exported %d rows (run %s)", table, rows, run_id)
    return rows


def export_silver(
    spark: SparkSession, root: str, run_id: str, tables: Iterable[str] = EXPORT_TABLES
) -> dict[str, int]:
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    counts = {t: export_table(spark, root, t, run_id) for t in tables}
    return {t: n for t, n in counts.items() if n is not None}
