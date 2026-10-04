"""Streams bronze clickstream Delta tables into Feast online features.

Queries: session_features → session_push, product_popularity → popularity_push.
"""

import logging
import os
import time
from collections.abc import Sequence
from functools import reduce

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.query import StreamingQuery

from lakehouse_spark.bronze.app import WatermarkDropLogger, build_session
from lakehouse_spark.realtime.popularity import POPULARITY_TYPES, product_popularity
from lakehouse_spark.realtime.push import push_batch
from lakehouse_spark.realtime.sessions import SESSION_TYPES, session_features
from lakehouse_spark.silver.domain.events import EVENT_TS_FORMAT

DISCOVERY_SECONDS = 30
CATEGORY_REFRESH_SECONDS = 600
log = logging.getLogger("realtime")


def events_path(root: str, event_type: str) -> str:
    return f"{root}/bronze/events/{event_type}"


def products_path(root: str) -> str:
    return f"{root}/silver/catalog/products"


def missing_tables(spark: SparkSession, paths: Sequence[str]) -> list[str]:
    return [p for p in paths if not DeltaTable.isDeltaTable(spark, p)]


def wait_for_tables(spark: SparkSession, paths: Sequence[str]) -> None:
    # a streaming union cannot gain sources later without a new checkpoint, so start once all exist
    while missing := missing_tables(spark, paths):
        log.info("waiting for Delta tables %s", missing)
        time.sleep(DISCOVERY_SECONDS)


def read_events(spark: SparkSession, root: str, types: Sequence[str]) -> DataFrame:
    streams = [
        spark.readStream.format("delta")
        .load(events_path(root, t))
        .select(
            "event_type",
            "session_id",
            "product_id",
            F.try_to_timestamp(F.col("event_ts"), F.lit(EVENT_TS_FORMAT)).alias("event_ts"),
        )
        for t in types
    ]
    # unparseable event_ts rows are quarantined by silver (event_cast_failed); they have no window
    return reduce(DataFrame.unionByName, streams).filter(F.col("event_ts").isNotNull())


def load_categories(spark: SparkSession, root: str) -> DataFrame:
    products = spark.read.format("delta").load(products_path(root))
    return (
        products.filter("is_current")
        .select("product_id", F.col("product_category_name").alias("category"))
        .dropDuplicates(["product_id"])
        .cache()
    )


def refresh_categories(categories: DataFrame) -> None:
    # running queries pick the re-cached snapshot up on their next micro-batch
    categories.unpersist(blocking=True)
    categories.cache()


def with_categories(events: DataFrame, categories: DataFrame) -> DataFrame:
    return events.join(F.broadcast(categories), "product_id", "left")


def start_push_query(
    features: DataFrame,
    *,
    name: str,
    source: str,
    key: str,
    root: str,
    base_url: str,
    trigger_seconds: int = 5,
) -> StreamingQuery:
    def process(batch: DataFrame, batch_id: int) -> None:
        push_batch(batch, batch_id, base_url=base_url, source=source, key=key)

    return (
        features.writeStream.queryName(name)
        .outputMode("update")
        .foreachBatch(process)
        .option("checkpointLocation", f"{root}/_checkpoints/realtime/{name}")
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )


def main() -> None:
    root = f"s3a://{os.environ['LAKEHOUSE_BUCKET']}"
    base_url = os.environ["FEAST_SERVER_URL"]
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    spark = build_session("realtime")
    spark.streams.addListener(WatermarkDropLogger())

    wait_for_tables(spark, [events_path(root, t) for t in POPULARITY_TYPES])
    start_push_query(
        product_popularity(read_events(spark, root, POPULARITY_TYPES)),
        name="product_popularity",
        source="popularity_push",
        key="product_id",
        root=root,
        base_url=base_url,
    )

    wait_for_tables(spark, [*(events_path(root, t) for t in SESSION_TYPES), products_path(root)])
    categories = load_categories(spark, root)
    start_push_query(
        session_features(with_categories(read_events(spark, root, SESSION_TYPES), categories)),
        name="session_features",
        source="session_push",
        key="session_id",
        root=root,
        base_url=base_url,
    )

    while not spark.streams.awaitAnyTermination(CATEGORY_REFRESH_SECONDS):
        refresh_categories(categories)
        log.info("refreshed product categories from silver")


if __name__ == "__main__":
    main()
