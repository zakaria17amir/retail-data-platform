import pytest
from lakehouse_spark.silver.domain.geo import GEO_RULES, zip_centroids
from lakehouse_spark.silver.rules import apply_rules
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

pytestmark = pytest.mark.spark


def test_geo_bbox_rejects_outside_brazil(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [
            (1, 1001, -23.5, -46.6, "sao paulo", "SP"),
            (2, 1002, -33.75, -73.99, "corner sw", "RS"),
            (3, 1003, 5.27, -34.79, "corner ne", "RR"),
            (4, 1004, 41.1, -8.6, "porto", "SP"),
            (5, 1005, -23.5, -34.78, "east of bbox", "SP"),
            (6, 1006, None, -46.6, "no lat", "SP"),
        ],
        "geolocation_pk long, geolocation_zip_code_prefix int, geolocation_lat double, "
        "geolocation_lng double, geolocation_city string, geolocation_state string",
    )
    kept, rejected, metrics = apply_rules(df, GEO_RULES)

    assert kept.columns == ["geolocation_pk", "zip_code_prefix", "lat", "lng", "city", "state"]
    assert sorted(r.geolocation_pk for r in kept.collect()) == [1, 2, 3]
    rows = rejected.select(
        "rule_id", F.get_json_object("record_json", "$.geolocation_pk").alias("pk")
    ).collect()
    assert {r.rule_id for r in rows} == {"geo_out_of_bbox"}
    assert sorted(r.pk for r in rows) == ["4", "5", "6"]
    assert metrics[-1].rule_id == "geo_out_of_bbox"
    assert (metrics[-1].rows_in, metrics[-1].rows_rejected) == (6, 3)


def test_zip_centroids_mean_and_mode_state(spark: SparkSession) -> None:
    points = spark.createDataFrame(
        [
            (1, 100, -10.0, -40.0, "a", "SP"),
            (2, 100, -12.0, -42.0, "a", "MG"),
            (3, 100, -14.0, -44.0, "a", "MG"),
            (4, 200, -20.0, -50.0, "b", "RJ"),
            (5, 200, -22.0, -52.0, "b", "ES"),
        ],
        "geolocation_pk long, zip_code_prefix int, lat double, lng double, city string, "
        "state string",
    )
    got = {
        r.zip_code_prefix: (r.lat, r.lng, r.n_points, r.state)
        for r in zip_centroids(points).collect()
    }
    assert got == {100: (-12.0, -42.0, 3, "MG"), 200: (-21.0, -51.0, 2, "ES")}
