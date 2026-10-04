import pytest
from lakehouse_spark.silver.domain.catalog import products_rules
from lakehouse_spark.silver.rules import apply_rules
from pyspark.sql import DataFrame, SparkSession

pytestmark = pytest.mark.spark


def _products(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [("p1", "beleza_saude", 10, 100), ("p2", None, 11, 101), ("p3", "nao_traduzida", 12, 102)],
        "product_id string, product_category_name string, "
        "product_name_lenght int, product_description_lenght int",
    )


def test_category_translate_fallback_unknown(spark: SparkSession) -> None:
    categories = spark.createDataFrame(
        [("beleza_saude", "health_beauty"), ("outra", "other")],
        "product_category_name string, product_category_name_english string",
    )
    kept, rejected, metrics = apply_rules(_products(spark), products_rules(categories))

    got = {r.product_id: r.product_category_name_english for r in kept.collect()}
    assert got == {"p1": "health_beauty", "p2": "unknown", "p3": "unknown"}
    assert rejected.count() == 0
    assert "category_translate" in [m.rule_id for m in metrics]

    no_categories, _, _ = apply_rules(_products(spark), products_rules(None))
    assert {r.product_category_name_english for r in no_categories.collect()} == {"unknown"}


def test_product_renames(spark: SparkSession) -> None:
    kept, _, _ = apply_rules(_products(spark), products_rules(None))

    assert "product_name_lenght" not in kept.columns
    assert "product_description_lenght" not in kept.columns
    row = kept.filter("product_id = 'p1'").collect()[0]
    assert (row.product_name_length, row.product_description_length) == (10, 100)
