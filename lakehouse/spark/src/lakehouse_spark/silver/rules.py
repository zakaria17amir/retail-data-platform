from collections.abc import Callable, Sequence
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class Rule:
    rule_id: str
    description: str
    fn: Callable[[DataFrame], tuple[DataFrame, DataFrame]]


@dataclass(frozen=True)
class RuleMetric:
    rule_id: str
    rows_in: int
    rows_rejected: int


def transform(rule_id: str, description: str, fn: Callable[[DataFrame], DataFrame]) -> Rule:
    def run(df: DataFrame) -> tuple[DataFrame, DataFrame]:
        return fn(df), df.limit(0).withColumn("reason", F.lit(None).cast("string"))

    return Rule(rule_id, description, run)


def apply_rules(
    df: DataFrame, rules: Sequence[Rule]
) -> tuple[DataFrame, DataFrame, list[RuleMetric]]:
    rejected_all = df.limit(0).select(
        F.lit(None).cast("string").alias("rule_id"),
        F.lit(None).cast("string").alias("reason"),
        F.lit(None).cast("string").alias("record_json"),
    )
    metrics = []
    for rule in rules:
        rows_in = df.count()
        kept, rejected = rule.fn(df)
        record = [c for c in rejected.columns if c != "reason"]
        rejected_all = rejected_all.unionByName(
            rejected.select(
                F.lit(rule.rule_id).alias("rule_id"),
                F.col("reason").cast("string").alias("reason"),
                F.to_json(F.struct(*record)).alias("record_json"),
            )
        )
        metrics.append(RuleMetric(rule.rule_id, rows_in, rejected.count()))
        df = kept
    return df, rejected_all, metrics
