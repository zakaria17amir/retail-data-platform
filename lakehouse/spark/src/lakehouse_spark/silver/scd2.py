from collections.abc import Sequence
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from lakehouse_spark.silver.cdc import on_keys

BEGINNING_OF_TIME = datetime(1900, 1, 1)


def _hash(tracked: Sequence[str]) -> Column:
    return F.sha2(F.to_json(F.struct(*tracked, "_is_deleted")), 256)


def scd2_new_versions(
    existing: DataFrame | None, changes: DataFrame, key: Sequence[str], tracked: Sequence[str]
) -> DataFrame:
    columns = [c for c in changes.columns if c not in ("_op", "_kafka_offset")]
    fresh = changes
    if existing is not None:
        fresh = fresh.join(existing, [*key, "_source_lsn"], "left_anti")
    fresh = fresh.withColumn("_hash", _hash(tracked)).withColumn("_new", F.lit(True))
    if existing is not None:
        history = existing.select(
            *key, "_source_lsn", _hash(tracked).alias("_hash"), F.lit(False).alias("_new")
        )
        fresh = fresh.unionByName(history, allowMissingColumns=True)
    prev = F.lag("_hash").over(Window.partitionBy(*key).orderBy("_source_lsn"))
    bot = F.lit(BEGINNING_OF_TIME.isoformat(sep=" ")).cast("timestamp")
    return (
        fresh.withColumn("_prev", prev)
        .filter(F.col("_new") & ~F.col("_hash").eqNullSafe(F.col("_prev")))
        .withColumn(
            "valid_from",
            F.when(F.col("_prev").isNull() & (F.col("_op") == "r"), bot).otherwise(
                F.col("_source_ts")
            ),
        )
        .select(*columns, "valid_from")
    )


def scd2_recompute(versions: DataFrame, key: Sequence[str]) -> DataFrame:
    valid_to = F.lead("valid_from").over(Window.partitionBy(*key).orderBy("_source_lsn"))
    return versions.withColumn("valid_to", valid_to).withColumn(
        "is_current", F.col("valid_to").isNull()
    )


def merge_scd2(
    spark: SparkSession, changes: DataFrame, path: str, key: Sequence[str], tracked: Sequence[str]
) -> int:
    if not DeltaTable.isDeltaTable(spark, path):
        new = scd2_new_versions(None, changes, key, tracked).cache()
        scd2_recompute(new, key).write.format("delta").save(path)
        inserted = new.count()
        new.unpersist()
        return inserted
    table = DeltaTable.forPath(spark, path)
    existing = table.toDF().join(changes.select(*key).distinct(), list(key), "left_semi")
    new = scd2_new_versions(existing, changes, key, tracked).cache()
    inserted = new.count()
    versions = scd2_recompute(existing.unionByName(new, allowMissingColumns=True), key)
    # one MERGE inserts new versions and re-closes neighbours atomically, so a rerun is a no-op
    (
        table.alias("t")
        .merge(versions.alias("s"), on_keys([*key, "_source_lsn"]))
        .whenMatchedUpdate(
            "NOT (t.valid_to <=> s.valid_to AND t.is_current <=> s.is_current)",
            {"valid_to": "s.valid_to", "is_current": "s.is_current"},
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    new.unpersist()
    return inserted
