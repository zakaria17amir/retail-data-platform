from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse_spark.silver.rules import Rule, transform

PRODUCT_RENAMES = {
    "product_name_lenght": "product_name_length",
    "product_description_lenght": "product_description_length",
}


def products_rules(categories: DataFrame | None) -> tuple[Rule, ...]:
    def translate(df: DataFrame) -> DataFrame:
        if categories is None:
            return df.withColumn("product_category_name_english", F.lit("unknown"))
        lookup = categories.select("product_category_name", "product_category_name_english")
        joined = df.join(F.broadcast(lookup), "product_category_name", "left")
        return joined.select(
            *df.columns,
            F.coalesce("product_category_name_english", F.lit("unknown")).alias(
                "product_category_name_english"
            ),
        )

    return (
        transform(
            "rename_columns",
            "fix Olist 'lenght' typos",
            lambda df: df.withColumnsRenamed(PRODUCT_RENAMES),
        ),
        transform(
            "category_translate",
            "English category name, 'unknown' when missing or untranslated",
            translate,
        ),
    )


CATEGORIES_RULES: tuple[Rule, ...] = ()
