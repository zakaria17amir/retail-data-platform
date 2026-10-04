from pyspark.sql import DataFrame
from pyspark.sql import functions as F

POPULARITY_TYPES = ("product_view", "add_to_cart")
WATERMARK = "25 hours"


def product_popularity(events: DataFrame) -> DataFrame:
    """24 h windows sliding by 1 h per product; a window's last hour is its 1 h window.

    Only rows whose newest event falls in the window's last hour are kept: that is the window
    "current" at the product's latest dataset time, so views_1h / views_24h describe the same now.
    """
    hour_start = F.col("window.end") - F.expr("INTERVAL 1 HOUR")
    last_hour = F.col("event_ts") >= hour_start
    view = F.col("event_type") == "product_view"
    return (
        events.filter(F.col("event_type").isin(*POPULARITY_TYPES) & F.col("product_id").isNotNull())
        .withWatermark("event_ts", WATERMARK)
        .withColumn("window", F.window("event_ts", "24 hours", "1 hour"))
        .groupBy("product_id", "window")
        .agg(
            F.count(F.when(view & last_hour, 1)).alias("views_1h"),
            F.count(F.when(view, 1)).alias("views_24h"),
            F.count(F.when(~view, 1)).alias("carts_24h"),
            F.max("event_ts").alias("event_ts"),
        )
        .filter(F.col("event_ts") >= hour_start)
        .drop("window")
    )
