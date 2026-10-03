import json
from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

from lakehouse_spark.registry import SchemaNotFound, SchemaSource

QUARANTINE_REASONS = (
    "null_payload",
    "not_wire_format",
    "unknown_schema_id",
    "avro_decode_failed",
    "null_primary_key",
    "unparseable_timestamp",
)

EVENT_TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"
MIN_EVENT_YEAR = 2016
# SIM_LATE_MAX_HOURS (48 h) + session/browsing offsets (<= ~2 h) + margin; later rows are dropped
DEDUPE_WATERMARK = "72 hours"

KAFKA_COLUMNS = ["kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp", "schema_id"]
INGEST_COLUMNS = ["ingest_ts", "ingest_date"]
QUARANTINE_COLUMNS = [*KAFKA_COLUMNS, "raw_value", "reason", *INGEST_COLUMNS]


def with_dedupe_key(df: DataFrame) -> DataFrame:
    fallback = F.concat_ws(
        "-", F.col("topic"), F.col("partition").cast("string"), F.col("offset").cast("string")
    )
    return df.withColumn("dedupe_key", F.coalesce(F.col("key").cast("string"), fallback))


def dedupe_stream(df: DataFrame) -> DataFrame:
    return df.withWatermark("timestamp", DEDUPE_WATERMARK).dropDuplicatesWithinWatermark(
        ["dedupe_key"]
    )


def _schema_id(value: Column) -> Column:
    is_wire = F.coalesce(
        (F.length(value) >= 5) & (F.hex(F.substring(value, 1, 1)) == "00"), F.lit(False)
    )
    return F.when(is_wire, F.conv(F.hex(F.substring(value, 2, 4)), 16, 10).cast("long"))


def _undecoded(schema_json: str) -> Column:
    # PERMISSIVE from_avro returns an all-null struct on failure: a null required field proves it
    fields = json.loads(schema_json).get("fields", [])
    required = next((f["name"] for f in fields if isinstance(f["type"], (str, dict))), None)
    if required is None:
        return F.col("_d").isNull()
    return F.col("_d").isNull() | F.col(f"_d.{required}").isNull()


def _quarantine(df: DataFrame, reason: Column) -> DataFrame:
    return df.select(
        *KAFKA_COLUMNS, F.col("_raw").alias("raw_value"), reason.alias("reason"), *INGEST_COLUMNS
    )


def _validation_reason(source: str) -> Column:
    if source == "events":
        parsed = F.expr(f'try_to_timestamp(event_ts, "{EVENT_TS_FORMAT}")')
        return F.when(
            F.col("event_id").isNull() | F.col("session_id").isNull(), "null_primary_key"
        ).when(
            parsed.isNull()
            | (F.year(parsed) < MIN_EVENT_YEAR)
            | (parsed > F.current_timestamp() + F.expr("INTERVAL 1 DAY")),
            "unparseable_timestamp",
        )
    return F.when(F.col("after").isNull() & F.col("before").isNull(), "null_payload")


def decode_batch(
    batch: DataFrame, registry: SchemaSource, source: str
) -> tuple[DataFrame, DataFrame]:
    base = (
        batch.select(
            F.col("value").alias("_raw"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            _schema_id(F.col("value")).alias("schema_id"),
        )
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("ingest_date", F.to_date("ingest_ts"))
    )

    quarantined = [
        _quarantine(base.filter(F.col("_raw").isNull()), F.lit("null_payload")),
        _quarantine(
            base.filter(F.col("_raw").isNotNull() & F.col("schema_id").isNull()),
            F.lit("not_wire_format"),
        ),
    ]

    wire = base.filter(F.col("schema_id").isNotNull())
    schema_ids = sorted(r.schema_id for r in wire.select("schema_id").distinct().collect())
    decoded: list[DataFrame] = []
    for schema_id in schema_ids:
        part = wire.filter(F.col("schema_id") == schema_id)
        try:
            schema_json = registry.get(schema_id)
        except SchemaNotFound:
            quarantined.append(_quarantine(part, F.lit("unknown_schema_id")))
            continue
        with_payload = part.withColumn(
            "_d",
            from_avro(
                F.expr("substring(_raw, 6, length(_raw) - 5)"), schema_json, {"mode": "PERMISSIVE"}
            ),
        )
        failed = _undecoded(schema_json)
        quarantined.append(_quarantine(with_payload.filter(failed), F.lit("avro_decode_failed")))
        decoded.append(
            with_payload.filter(~failed).select("_d.*", "_raw", *KAFKA_COLUMNS, *INGEST_COLUMNS)
        )

    meta = [*KAFKA_COLUMNS, *INGEST_COLUMNS]
    if decoded:
        unioned = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), decoded)
        checked = unioned.withColumn("_reason", _validation_reason(source))
        good = checked.filter(F.col("_reason").isNull()).drop("_reason", "_raw")
        quarantined.append(
            _quarantine(checked.filter(F.col("_reason").isNotNull()), F.col("_reason"))
        )
    else:
        good = base.filter(F.lit(False)).select(*meta)
    bad = reduce(lambda a, b: a.unionByName(b), quarantined).select(*QUARANTINE_COLUMNS)
    return good, bad
