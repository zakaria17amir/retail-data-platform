import json

import pytest
from delta.tables import DeltaTable
from lakehouse_spark.silver.maintain import optimize_and_vacuum
from pyspark.sql import SparkSession

pytestmark = pytest.mark.spark


def test_optimize_reduces_files(spark: SparkSession, root: str) -> None:
    orders = f"{root}/silver/sales/orders"
    bronze = f"{root}/bronze/olist/orders"
    for i in range(3):
        row = spark.createDataFrame([(f"o{i}", f"c{i}")], "order_id string, customer_id string")
        row.write.format("delta").mode("append").save(orders)
        row.write.format("delta").mode("append").save(bronze)

    assert optimize_and_vacuum(spark, root) == {orders: 1, bronze: 1}
    history = DeltaTable.forPath(spark, orders).history().collect()
    optimize = next(h for h in history if h.operation == "OPTIMIZE")
    assert json.loads(optimize.operationParameters["zOrderBy"]) == ["customer_id"]
    assert spark.read.format("delta").load(orders).count() == 3
