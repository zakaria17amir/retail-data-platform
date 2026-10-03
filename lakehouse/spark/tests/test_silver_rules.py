from datetime import datetime

import pytest
from lakehouse_spark.silver.rules import Rule, apply_rules, transform
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

pytestmark = pytest.mark.spark


def _reject_odd(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    odd = F.col("n") % 2 == 1
    return df.filter(~odd), df.filter(odd).withColumn("reason", F.lit("odd"))


def _reject_big(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    big = F.col("n") > 6
    return df.filter(~big), df.filter(big).withColumn("reason", F.lit("big"))


def test_apply_rules_accounting(spark: SparkSession) -> None:
    ts = datetime(2017, 1, 15, 10, 0, 0)
    df = spark.createDataFrame(
        [(n, f"id{n}", ts) for n in range(1, 11)], "n int, name string, ts timestamp"
    )
    rules = [
        Rule("odd", "reject odd n", _reject_odd),
        transform("double", "double n", lambda d: d.withColumn("n2", F.col("n") * 2)),
        Rule("big", "reject n > 6", _reject_big),
    ]

    kept, rejected, metrics = apply_rules(df, rules)

    assert [(m.rule_id, m.rows_in, m.rows_rejected) for m in metrics] == [
        ("odd", 10, 5),
        ("double", 5, 0),
        ("big", 5, 2),
    ]
    assert kept.count() == 3
    assert sorted(r.n for r in kept.collect()) == [2, 4, 6]
    assert "n2" in kept.columns
    assert rejected.columns == ["rule_id", "reason", "record_json"]
    rows = rejected.collect()
    assert sorted((r.rule_id, r.reason) for r in rows) == sorted(
        [("odd", "odd")] * 5 + [("big", "big")] * 2
    )
    parsed = rejected.select(
        "rule_id", F.from_json("record_json", "n int, name string, ts timestamp, n2 int").alias("r")
    ).collect()
    odd_records = sorted((p.r.n, p.r.name, p.r.ts) for p in parsed if p.rule_id == "odd")
    assert odd_records == [(n, f"id{n}", ts) for n in (1, 3, 5, 7, 9)]
    big_records = sorted((p.r.n, p.r.n2) for p in parsed if p.rule_id == "big")
    assert big_records == [(8, 16), (10, 20)]
