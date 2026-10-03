import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.query import StreamingQuery

from lakehouse_spark.bronze.decode import decode_batch, dedupe_stream, with_dedupe_key
from lakehouse_spark.bronze.sink import write_bronze, write_quarantine
from lakehouse_spark.registry import SchemaRegistry

METADATA_REFRESH_MS = "30000"


def build_session(app_name: str = "bronze") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def start_query(
    spark: SparkSession,
    *,
    name: str,
    subscribe_pattern: str,
    source: str,
    root: str,
    registry: SchemaRegistry,
    bootstrap: str,
    trigger_seconds: int = 5,
    max_offsets: int = 50_000,
    dedupe: bool,
) -> StreamingQuery:
    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribePattern", subscribe_pattern)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", max_offsets)
        .option("kafka.metadata.max.age.ms", METADATA_REFRESH_MS)
        .load()
    )
    if dedupe:
        stream = dedupe_stream(with_dedupe_key(stream))

    def process(batch: DataFrame, batch_id: int) -> None:
        batch.persist()
        try:
            topics = sorted(r.topic for r in batch.select("topic").distinct().collect())
            for topic in topics:
                good, bad = decode_batch(batch.filter(F.col("topic") == topic), registry, source)
                write_bronze(good, root, source, name, batch_id)
                if not bad.isEmpty():
                    write_quarantine(bad, root, source, name, batch_id)
        finally:
            batch.unpersist()

    return (
        stream.writeStream.queryName(name)
        .foreachBatch(process)
        .option("checkpointLocation", f"{root}/_checkpoints/bronze/{name}")
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )


def main() -> None:
    root = f"s3a://{os.environ['LAKEHOUSE_BUCKET']}"
    bootstrap = os.environ["KAFKA_BOOTSTRAP"]
    registry = SchemaRegistry(os.environ["SCHEMA_REGISTRY_URL"])
    spark = build_session()
    for name, pattern, source, dedupe in (
        ("cdc_to_bronze", r"cdc\.olist\..*", "olist", False),
        ("events_to_bronze", r"events\..*", "events", True),
    ):
        start_query(
            spark,
            name=name,
            subscribe_pattern=pattern,
            source=source,
            root=root,
            registry=registry,
            bootstrap=bootstrap,
            dedupe=dedupe,
        )
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
