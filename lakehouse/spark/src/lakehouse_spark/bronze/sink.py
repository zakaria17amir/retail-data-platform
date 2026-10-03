from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def bronze_path(root: str, source: str, topic: str) -> str:
    return f"{root}/bronze/{source}/{topic.rsplit('.', 1)[-1]}"


def _append(df: DataFrame, path: str, txn_app_id: str | None, txn_version: int | None) -> None:
    writer = (
        df.write.format("delta")
        .mode("append")
        .partitionBy("ingest_date")
        .option("mergeSchema", "true")
    )
    if txn_app_id is not None and txn_version is not None:
        writer = writer.option("txnAppId", txn_app_id).option("txnVersion", txn_version)
    writer.save(path)


def write_bronze(
    good: DataFrame,
    root: str,
    source: str,
    txn_app_id: str | None = None,
    txn_version: int | None = None,
) -> None:
    topics = sorted(r.kafka_topic for r in good.select("kafka_topic").distinct().collect())
    for topic in topics:
        _append(
            good.filter(F.col("kafka_topic") == topic),
            bronze_path(root, source, topic),
            None if txn_app_id is None else f"{txn_app_id}-{topic}",
            txn_version,
        )


def write_quarantine(
    bad: DataFrame,
    root: str,
    source: str,
    txn_app_id: str | None = None,
    txn_version: int | None = None,
) -> None:
    _append(
        bad,
        f"{root}/bronze/_quarantine/{source}",
        None if txn_app_id is None else f"{txn_app_id}-quarantine",
        txn_version,
    )
