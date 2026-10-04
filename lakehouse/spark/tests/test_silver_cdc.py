from typing import Any

import pytest
from lakehouse_spark.silver.cdc import collapse_latest, exact_duplicates, flatten_cdc, merge_current
from pyspark.sql import SparkSession

from tests.helpers import bronze, cdc, source_ts

pytestmark = pytest.mark.spark


def test_flatten_uses_before_for_deletes(spark: SparkSession) -> None:
    flat = flatten_cdc(bronze(spark, [cdc("c", "a", "x", 1, 0), cdc("d", "a", "x", 2, 1)]))
    assert flat.columns == [
        "id",
        "name",
        "_op",
        "_source_lsn",
        "_source_ts",
        "_is_deleted",
        "_kafka_offset",
    ]
    rows = sorted(flat.collect(), key=lambda r: r._source_lsn)
    assert [tuple(r) for r in rows] == [
        ("a", "x", "c", 1, source_ts(1), False, 0),
        ("a", "x", "d", 2, source_ts(2), True, 1),
    ]


def test_exact_duplicates_rejects_same_key_and_lsn_only(spark: SparkSession) -> None:
    flat = flatten_cdc(
        bronze(
            spark,
            [
                cdc("u", "a", "x", 5, 7),
                cdc("u", "a", "x", 5, 3),
                cdc("u", "a", "y", 6, 4),
                cdc("u", "b", "x", 5, 5),
            ],
        )
    )
    kept, rejected = exact_duplicates(flat, ["id"])
    assert sorted((r.id, r._source_lsn, r._kafka_offset) for r in kept.collect()) == [
        ("a", 5, 3),
        ("a", 6, 4),
        ("b", 5, 5),
    ]
    assert [(r.id, r._kafka_offset, r.reason) for r in rejected.collect()] == [
        ("a", 7, "duplicate delivery of key+lsn")
    ]


def test_exact_duplicates_rejects_key_and_lsn_already_in_silver(spark: SparkSession) -> None:
    existing = flatten_cdc(bronze(spark, [cdc("r", "a", "x", 1, 0)]))
    flat = flatten_cdc(bronze(spark, [cdc("r", "a", "x", 1, 5), cdc("u", "a", "y", 2, 6)]))
    kept, rejected = exact_duplicates(flat, ["id"], existing)
    assert [(r.id, r._source_lsn) for r in kept.collect()] == [("a", 2)]
    assert [(r.id, r._kafka_offset, r.reason) for r in rejected.collect()] == [
        ("a", 5, "duplicate delivery of key+lsn (already in silver)")
    ]
    assert rejected.columns == [*flat.columns, "reason"]


def test_collapse_latest_picks_max_lsn_then_offset(spark: SparkSession) -> None:
    flat = flatten_cdc(
        bronze(
            spark,
            [
                cdc("u", "a", "x", 9, 1),
                cdc("u", "a", "y", 3, 8),
                cdc("u", "b", "x", 4, 2),
                cdc("u", "b", "z", 4, 6),
            ],
        )
    )
    latest = collapse_latest(flat, ["id"])
    assert sorted((r.id, r.name) for r in latest.collect()) == [("a", "x"), ("b", "z")]


def _current(spark: SparkSession, path: str) -> list[tuple[Any, ...]]:
    return sorted(tuple(r) for r in spark.read.format("delta").load(path).collect())


def test_merge_current_latest_wins_regardless_of_batch_order(
    spark: SparkSession, root: str
) -> None:
    path = f"{root}/silver/t"
    newer = flatten_cdc(bronze(spark, [cdc("u", "a", "new", 9, 9), cdc("c", "b", "b1", 8, 8)]))
    older = flatten_cdc(bronze(spark, [cdc("c", "a", "old", 1, 1), cdc("c", "c", "c1", 2, 2)]))
    merge_current(spark, collapse_latest(newer, ["id"]), path, ["id"])
    merge_current(spark, collapse_latest(older, ["id"]), path, ["id"])
    table = spark.read.format("delta").load(path)
    assert "_op" not in table.columns and "_kafka_offset" not in table.columns
    assert sorted((r.id, r.name, r._source_lsn) for r in table.collect()) == [
        ("a", "new", 9),
        ("b", "b1", 8),
        ("c", "c1", 2),
    ]


def test_merge_current_rerun_is_noop(spark: SparkSession, root: str) -> None:
    path = f"{root}/silver/t"
    first = flatten_cdc(bronze(spark, [cdc("r", "a", "x", 1, 0), cdc("r", "b", "y", 2, 1)]))
    merge_current(spark, collapse_latest(first, ["id"]), path, ["id"])
    batch = collapse_latest(
        flatten_cdc(bronze(spark, [cdc("u", "a", "x2", 3, 2), cdc("d", "b", "y", 4, 3)])), ["id"]
    )
    merge_current(spark, batch, path, ["id"])
    once = _current(spark, path)
    merge_current(spark, batch, path, ["id"])
    assert _current(spark, path) == once
    assert [(r[0], r[1], r[4]) for r in once] == [("a", "x2", False), ("b", "y", True)]
