from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from lakehouse_spark.silver.domain.sales import localise
from lakehouse_spark.silver.rules import Rule, transform

EVENT_TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"


def _parsed_ts() -> Column:
    return F.try_to_timestamp(F.col("event_ts"), F.lit(EVENT_TS_FORMAT))


def _cast_failed(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    ok = _parsed_ts().isNotNull()
    return df.filter(ok), df.filter(~ok).withColumn(
        "reason", F.lit(f"event_ts is not {EVENT_TS_FORMAT}")
    )


def _localise(df: DataFrame) -> DataFrame:
    out = localise(df.withColumn("event_ts", _parsed_ts()), {"event_ts": "event_ts"})
    return out.withColumn("event_date", F.to_date("event_ts_local"))


def _negative_quantity(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    bad = F.coalesce(F.col("quantity") < 0, F.lit(False))
    return df.filter(~bad), df.filter(bad).withColumn("reason", F.lit("quantity < 0"))


def event_rules(existing: DataFrame | None) -> tuple[Rule, ...]:
    def duplicate(df: DataFrame) -> tuple[DataFrame, DataFrame]:
        first = Window.partitionBy("event_id").orderBy("_bronze_ingest_ts", "kafka_offset")
        ranked = df.withColumn("_rn", F.row_number().over(first))
        if existing is None:
            ranked = ranked.withColumn("_seen", F.lit(False))
        else:
            seen = existing.select("event_id").distinct().withColumn("_seen", F.lit(True))
            ranked = ranked.join(seen, "event_id", "left")
        in_silver = F.coalesce(F.col("_seen"), F.lit(False))
        dup = in_silver | (F.col("_rn") > 1)
        reason = F.when(in_silver, "event_id already in silver").otherwise(
            "event_id seen earlier in batch"
        )
        return ranked.filter(~dup).select(*df.columns), ranked.filter(dup).select(
            *df.columns, reason.alias("reason")
        )

    def over_24h(df: DataFrame) -> tuple[DataFrame, DataFrame]:
        starts = df.select("session_id", "event_ts_local")
        if existing is not None:
            starts = starts.unionByName(existing.select("session_id", "event_ts_local"))
        starts = starts.groupBy("session_id").agg(F.min("event_ts_local").alias("_start"))
        joined = df.join(starts, "session_id", "left")
        late = F.coalesce(
            F.col("event_ts_local") > F.col("_start") + F.expr("INTERVAL 24 HOURS"), F.lit(False)
        )
        return joined.filter(~late).select(*df.columns), joined.filter(late).select(
            *df.columns, F.lit("event more than 24 h after session start").alias("reason")
        )

    return (
        Rule("event_cast_failed", "event_ts unparseable", _cast_failed),
        transform(
            "ts_localise", "event_ts → event_ts_local (ntz), event_ts_utc, event_date", _localise
        ),
        Rule("event_negative_quantity", "quantity below zero", _negative_quantity),
        Rule("event_duplicate", "event_id repeated in batch or already in silver", duplicate),
        Rule("session_over_24h", "event beyond 24 h of its session start", over_24h),
    )
