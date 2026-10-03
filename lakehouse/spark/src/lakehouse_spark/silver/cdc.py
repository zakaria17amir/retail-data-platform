from collections.abc import Sequence

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

META = ("_source_lsn", "_source_ts", "_is_deleted")
DUPLICATE_REASON = "duplicate delivery of key+lsn"


def flatten_cdc(bronze: DataFrame) -> DataFrame:
    after = bronze.schema["after"].dataType
    assert isinstance(after, StructType)
    row = F.coalesce(F.col("after"), F.col("before"))
    return bronze.select(
        *[row[f.name].alias(f.name) for f in after.fields if f.name != "updated_at"],
        F.col("op").alias("_op"),
        F.col("source.lsn").alias("_source_lsn"),
        F.timestamp_millis(F.col("source.ts_ms")).alias("_source_ts"),
        (F.col("op") == "d").alias("_is_deleted"),
        F.col("kafka_offset").alias("_kafka_offset"),
    )


def exact_duplicates(df: DataFrame, key: Sequence[str]) -> tuple[DataFrame, DataFrame]:
    window = Window.partitionBy(*key, "_source_lsn").orderBy("_kafka_offset")
    ranked = df.withColumn("_rn", F.row_number().over(window))
    kept = ranked.filter(F.col("_rn") == 1).drop("_rn")
    rejected = (
        ranked.filter(F.col("_rn") > 1).drop("_rn").withColumn("reason", F.lit(DUPLICATE_REASON))
    )
    return kept, rejected


def collapse_latest(df: DataFrame, key: Sequence[str]) -> DataFrame:
    window = Window.partitionBy(*key).orderBy(F.desc("_source_lsn"), F.desc("_kafka_offset"))
    return df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")


def on_keys(key: Sequence[str]) -> str:
    return " AND ".join(f"t.`{k}` = s.`{k}`" for k in key)


def merge_current(spark: SparkSession, latest: DataFrame, path: str, key: Sequence[str]) -> None:
    rows = latest.drop("_op", "_kafka_offset")
    if not DeltaTable.isDeltaTable(spark, path):
        rows.write.format("delta").save(path)
        return
    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(rows.alias("s"), on_keys(key))
        .whenMatchedUpdateAll("s._source_lsn >= t._source_lsn")
        .whenNotMatchedInsertAll()
        .execute()
    )
