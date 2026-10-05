"""Bronze → silver: one availableNow Delta stream per bronze source, applied with foreachBatch.

Every write of a micro-batch (rejects, rule metrics, silver MERGE) carries the Delta txnAppId
silver-<checkpoint name>-<streaming query id> and txnVersion (batch id), so a batch replayed after a
crash between the writes and the checkpoint commit is skipped. The query id is stored in the
checkpoint, so a deleted or reset checkpoint starts a fresh transaction namespace and its batches
(re-numbered from 0) are written instead of being skipped. Without clearing silver too, such a reset
reprocesses all of bronze: silver converges (MERGE), rejects and metrics are appended again, and
records already in silver are re-rejected (cdc_exact_duplicate / event_duplicate).
"""

import argparse
import logging
import os
import secrets
import threading
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.errors import StreamingQueryException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from lakehouse_spark.bronze.app import build_session
from lakehouse_spark.cli import lakehouse_uri
from lakehouse_spark.silver.cdc import collapse_latest, exact_duplicates, flatten_cdc, merge_current
from lakehouse_spark.silver.domain.events import event_rules
from lakehouse_spark.silver.domain.geo import zip_centroids
from lakehouse_spark.silver.rules import Rule, RuleMetric, apply_rules, transform
from lakehouse_spark.silver.scd2 import merge_scd2
from lakehouse_spark.silver.tables import TABLES, TableSpec, read_if_exists

MAX_ATTEMPTS = 3
SCHEMA_CHANGE_MARKERS = ("DELTA_SCHEMA_CHANGED", "Detected schema change")
TXN_APP_ID = "spark.databricks.delta.write.txnAppId"
TXN_VERSION = "spark.databricks.delta.write.txnVersion"
EVENT_FIELDS = (
    "event_id",
    "event_type",
    "session_id",
    "customer_id",
    "device",
    "referrer",
    "event_ts",
    "product_id",
    "search_query",
    "quantity",
    "order_id",
    "utm_campaign",
    "rank",
    "rec_model_version",
    "rec_strategy",
)
INT_EVENT_FIELDS = ("quantity", "rank")
# v3 columns last: Delta schema evolution appends them, so new and evolved tables share one order
EVENT_COLUMNS = (
    "event_id",
    "event_type",
    "session_id",
    "customer_id",
    "device",
    "referrer",
    "event_ts_local",
    "event_ts_utc",
    "product_id",
    "search_query",
    "quantity",
    "order_id",
    "utm_campaign",
    "event_date",
    "_bronze_ingest_ts",
    "_silver_loaded_at",
    "_run_id",
    "rank",
    "rec_model_version",
    "rec_strategy",
)
EVENT_BRONZE_SCHEMA = StructType(
    [
        *(
            StructField(c, IntegerType() if c in INT_EVENT_FIELDS else StringType())
            for c in EVENT_FIELDS
        ),
        StructField("ingest_ts", TimestampType()),
        StructField("kafka_partition", IntegerType()),
        StructField("kafka_offset", LongType()),
    ]
)
METRICS_SCHEMA = StructType(
    [
        StructField("run_id", StringType()),
        StructField("table", StringType()),
        StructField("rule_id", StringType()),
        StructField("rows_in", LongType()),
        StructField("rows_rejected", LongType()),
        StructField("pct_rejected", DoubleType()),
        StructField("run_ts", TimestampType()),
    ]
)
UTC = timezone.utc  # noqa: UP017 - the spark image runs Python 3.10 (no datetime.UTC)
MAX_BYTES_PER_TRIGGER = "256m"
log = logging.getLogger("silver")


def checkpoint_name(spec: TableSpec, source: str) -> str:
    name = spec.name.replace("/", "_")
    return f"{name}_{source.rsplit('/', 1)[-1]}" if len(spec.bronze) > 1 else name


def cdc_rules(spec: TableSpec, spark: SparkSession, silver: str) -> tuple[Rule, ...]:
    existing = read_if_exists(spark, f"{silver}/{spec.name}")
    dedupe = Rule(
        "cdc_exact_duplicate",
        "redelivered CDC record (same key and LSN, in the batch or already in silver)",
        lambda df: exact_duplicates(df, spec.key, existing),
    )
    if spec.mode == "scd2":
        return (dedupe,)
    collapse = transform(
        "cdc_collapse", "latest version per key", lambda df: collapse_latest(df, spec.key)
    )
    return dedupe, collapse


def events_input(bronze: DataFrame) -> DataFrame:
    fields = [
        F.col(c)
        if c in bronze.columns
        else F.lit(None).cast(EVENT_BRONZE_SCHEMA[c].dataType).alias(c)
        for c in EVENT_FIELDS
    ]
    return bronze.select(
        *fields, F.col("ingest_ts").alias("_bronze_ingest_ts"), "kafka_partition", "kafka_offset"
    )


def merge_events(spark: SparkSession, rows: DataFrame, path: str) -> None:
    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(rows.alias("s"), "t.event_id = s.event_id")
        .whenNotMatchedInsertAll()
        .execute()
    )


def _metrics_rows(
    spark: SparkSession, spec: TableSpec, run_id: str, metrics: list[RuleMetric]
) -> DataFrame:
    now = datetime.now(UTC)
    rows = [
        (
            run_id,
            spec.name,
            m.rule_id,
            m.rows_in,
            m.rows_rejected,
            100.0 * m.rows_rejected / m.rows_in if m.rows_in else 0.0,
            now,
        )
        for m in metrics
    ]
    return spark.createDataFrame(rows, METRICS_SCHEMA)


def with_load_meta(df: DataFrame, run_id: str) -> DataFrame:
    return df.withColumns({"_silver_loaded_at": F.current_timestamp(), "_run_id": F.lit(run_id)})


def ensure_events_table(spark: SparkSession, path: str, run_id: str) -> None:
    """Create the events table, or add contract columns it lacks (Delta schema evolution)."""
    existing = read_if_exists(spark, path)
    if existing is not None and set(EVENT_COLUMNS) <= set(existing.columns):
        return
    # same lineage as a real batch, so the schema matches what later MERGEs insert
    empty = spark.createDataFrame([], EVENT_BRONZE_SCHEMA)
    kept, _, _ = apply_rules(events_input(empty), event_rules(None))
    rows = with_load_meta(kept, run_id).select(*EVENT_COLUMNS)
    rows.write.format("delta").mode("append").option("mergeSchema", "true").partitionBy(
        "event_date"
    ).save(path)


def process_batch(
    batch: DataFrame, batch_id: int, *, spec: TableSpec, root: str, run_id: str, app_id: str
) -> int:
    spark = batch.sparkSession
    silver = f"{root}/silver"
    batch.persist()
    kept = rejected = None
    try:
        rows_in = batch.count()
        if rows_in == 0:
            return 0
        if spec.mode == "events":
            df, chain = events_input(batch), spec.rules(spark, silver)
        else:
            df = flatten_cdc(batch)
            chain = (*cdc_rules(spec, spark, silver), *spec.rules(spark, silver))
        kept, rejected, metrics = apply_rules(df, chain)
        kept = with_load_meta(kept, run_id).persist()
        rejected = rejected.withColumns(
            {"_run_id": F.lit(run_id), "_rejected_at": F.current_timestamp()}
        ).persist()
        # materialise before any write: event rules read the silver table the merge changes
        kept.count()
        n_rejected = rejected.count()

        spark.conf.set(TXN_APP_ID, app_id)
        spark.conf.set(TXN_VERSION, str(batch_id))
        if n_rejected:
            rejected.write.format("delta").mode("append").save(f"{silver}/_rejects/{spec.name}")
        _metrics_rows(spark, spec, run_id, metrics).write.format("delta").mode("append").save(
            f"{silver}/_rule_metrics"
        )
        path = f"{silver}/{spec.name}"
        if spec.mode == "events":
            merge_events(spark, kept.select(*EVENT_COLUMNS), path)
        elif spec.mode == "scd2":
            merge_scd2(spark, kept, path, spec.key, spec.tracked)
        else:
            merge_current(spark, kept, path, spec.key)
        log.info("%s batch %d: %d rows in, %d rejected", app_id, batch_id, rows_in, n_rejected)
        return rows_in
    finally:
        spark.conf.unset(TXN_APP_ID)
        spark.conf.unset(TXN_VERSION)
        for frame in (kept, rejected, batch):
            if frame is not None:
                frame.unpersist()


def is_schema_change(error: Exception) -> bool:
    return any(marker in str(error) for marker in SCHEMA_CHANGE_MARKERS)


def run_source(spark: SparkSession, spec: TableSpec, source: str, root: str, run_id: str) -> int:
    name = checkpoint_name(spec, source)
    processed = [0]
    query_id: list[str] = []
    started = threading.Event()

    def process(batch: DataFrame, batch_id: int) -> None:
        started.wait()
        app_id = f"silver-{name}-{query_id[-1]}"
        processed[0] += process_batch(
            batch, batch_id, spec=spec, root=root, run_id=run_id, app_id=app_id
        )

    for attempt in range(1, MAX_ATTEMPTS + 1):
        started.clear()
        query = (
            spark.readStream.format("delta")
            .option("maxBytesPerTrigger", MAX_BYTES_PER_TRIGGER)
            .load(f"{root}/bronze/{source}")
            .writeStream.queryName(f"silver-{name}")
            .option("checkpointLocation", f"{root}/_checkpoints/silver/{name}")
            .trigger(availableNow=True)
            .foreachBatch(process)
            .start()
        )
        query_id.append(str(query.id))
        started.set()
        try:
            query.awaitTermination()
            break
        except StreamingQueryException as e:
            if attempt == MAX_ATTEMPTS or not is_schema_change(e):
                raise
            log.warning("%s: bronze schema changed, restarting (attempt %d)", name, attempt + 1)
    return processed[0]


def run_table(spark: SparkSession, spec: TableSpec, root: str, run_id: str) -> None:
    processed = 0
    path = f"{root}/silver/{spec.name}"
    if spec.mode == "events":
        ensure_events_table(spark, path, run_id)
    for source in spec.bronze:
        if not DeltaTable.isDeltaTable(spark, f"{root}/bronze/{source}"):
            log.info("%s: bronze/%s does not exist yet, skipped", spec.name, source)
            continue
        processed += run_source(spark, spec, source, root, run_id)
    if spec.name == "geo/geolocation_points":
        points = read_if_exists(spark, path)
        centroids_path = f"{root}/silver/geo/zip_centroids"
        if points is not None and (processed or not DeltaTable.isDeltaTable(spark, centroids_path)):
            centroids = zip_centroids(points.filter(~F.col("_is_deleted")))
            centroids.write.format("delta").mode("overwrite").save(centroids_path)


def new_run_id() -> str:
    return f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{secrets.token_hex(3)}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="silver")
    parser.add_argument("--tables", default="", help="comma-separated silver tables (default all)")
    parser.add_argument("--root", default=lakehouse_uri(os.environ))
    args = parser.parse_args(argv)
    names = [n for n in args.tables.split(",") if n]
    unknown = set(names) - {spec.name for spec in TABLES}
    if unknown:
        parser.error(f"unknown tables: {', '.join(sorted(unknown))}")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    spark = build_session("silver")
    run_id = new_run_id()
    log.info("silver run %s", run_id)
    for spec in TABLES:
        if not names or spec.name in names:
            run_table(spark, spec, args.root.rstrip("/"), run_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
