from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from lakehouse_spark.silver.rules import Rule, transform

BRAZIL_BBOX = {"lat_min": -33.75, "lat_max": 5.27, "lng_min": -73.99, "lng_max": -34.79}
GEO_RENAMES = {f"geolocation_{c}": c for c in ("zip_code_prefix", "lat", "lng", "city", "state")}


def _out_of_bbox(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    inside = F.coalesce(
        F.col("lat").between(BRAZIL_BBOX["lat_min"], BRAZIL_BBOX["lat_max"])
        & F.col("lng").between(BRAZIL_BBOX["lng_min"], BRAZIL_BBOX["lng_max"]),
        F.lit(False),
    )
    return df.filter(inside), df.filter(~inside).withColumn(
        "reason", F.lit("lat/lng missing or outside Brazil bounding box")
    )


GEO_RULES: tuple[Rule, ...] = (
    transform(
        "rename_columns",
        "drop geolocation_ prefix",
        lambda df: df.withColumnsRenamed(GEO_RENAMES),
    ),
    Rule("geo_out_of_bbox", "point outside Brazil", _out_of_bbox),
)


def zip_centroids(points: DataFrame) -> DataFrame:
    means = points.groupBy("zip_code_prefix").agg(
        F.avg("lat").alias("lat"), F.avg("lng").alias("lng"), F.count("*").alias("n_points")
    )
    by_state = points.groupBy("zip_code_prefix", "state").count()
    rank = Window.partitionBy("zip_code_prefix").orderBy(F.desc("count"), F.asc_nulls_last("state"))
    mode = (
        by_state.withColumn("_rn", F.row_number().over(rank))
        .filter("_rn = 1")
        .select("zip_code_prefix", "state")
    )
    return means.join(mode, "zip_code_prefix", "left")
