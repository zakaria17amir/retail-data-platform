from typing import Any

import pytest
from lakehouse_spark.silver.cdc import flatten_cdc
from lakehouse_spark.silver.scd2 import BEGINNING_OF_TIME, merge_scd2
from pyspark.sql import Row, SparkSession

from tests.helpers import bronze, cdc, source_ts

pytestmark = pytest.mark.spark

KEY = ["id"]
TRACKED = ["name"]


def _merge(spark: SparkSession, path: str, rows: list[tuple[Any, ...]]) -> int:
    return merge_scd2(spark, flatten_cdc(bronze(spark, rows)), path, KEY, TRACKED)


def _versions(spark: SparkSession, path: str) -> list[Row]:
    rows = spark.read.format("delta").load(path).collect()
    return sorted(rows, key=lambda r: (r.id, r._source_lsn))


def _assert_contiguous(versions: list[Row]) -> None:
    for prev, nxt in zip(versions, versions[1:], strict=False):
        assert prev.valid_to == nxt.valid_from
        assert prev.is_current is False
    assert versions[-1].valid_to is None
    assert versions[-1].is_current is True


def test_scd2_noop_update_creates_no_version(spark: SparkSession, root: str) -> None:
    path = f"{root}/silver/t"
    assert _merge(spark, path, [cdc("r", "a", "x", 1, 0)]) == 1
    assert _merge(spark, path, [cdc("u", "a", "x", 2, 1, updated=5)]) == 0
    versions = _versions(spark, path)
    assert [(r._source_lsn, r.is_current, r.valid_to) for r in versions] == [(1, True, None)]


def test_scd2_change_closes_previous_and_opens_new(spark: SparkSession, root: str) -> None:
    path = f"{root}/silver/t"
    _merge(spark, path, [cdc("r", "a", "x", 1, 0)])
    assert _merge(spark, path, [cdc("u", "a", "y", 2, 1)]) == 1
    versions = _versions(spark, path)
    assert [(r.name, r.valid_from) for r in versions] == [
        ("x", BEGINNING_OF_TIME),
        ("y", source_ts(2)),
    ]
    _assert_contiguous(versions)
    assert "_op" not in versions[0] and "_kafka_offset" not in versions[0]


def test_scd2_snapshot_version_starts_at_beginning_of_time(spark: SparkSession, root: str) -> None:
    path = f"{root}/silver/t"
    _merge(spark, path, [cdc("r", "a", "x", 1, 0), cdc("c", "b", "y", 2, 1)])
    assert [(r.id, r.valid_from) for r in _versions(spark, path)] == [
        ("a", BEGINNING_OF_TIME),
        ("b", source_ts(2)),
    ]


def test_scd2_multiple_versions_in_one_batch(spark: SparkSession, root: str) -> None:
    path = f"{root}/silver/t"
    _merge(spark, path, [cdc("r", "a", "x", 1, 0)])
    batch = [
        cdc("u", "a", "y", 2, 1),
        cdc("u", "a", "y", 3, 2, updated=9),
        cdc("u", "a", "z", 4, 3),
    ]
    assert _merge(spark, path, batch) == 2
    versions = _versions(spark, path)
    assert [(r.name, r._source_lsn) for r in versions] == [("x", 1), ("y", 2), ("z", 4)]
    _assert_contiguous(versions)


def test_scd2_rerun_same_batch_is_noop(spark: SparkSession, root: str) -> None:
    path = f"{root}/silver/t"
    _merge(spark, path, [cdc("r", "a", "x", 1, 0), cdc("r", "b", "q", 2, 1)])
    batch = [cdc("u", "a", "y", 3, 2), cdc("u", "b", "q", 4, 3, updated=1)]
    assert _merge(spark, path, batch) == 1
    once = _versions(spark, path)
    assert _merge(spark, path, batch) == 0
    assert _versions(spark, path) == once
    assert len(once) == 3


def test_scd2_delete_is_a_version(spark: SparkSession, root: str) -> None:
    path = f"{root}/silver/t"
    _merge(spark, path, [cdc("r", "a", "x", 1, 0)])
    assert _merge(spark, path, [cdc("d", "a", "x", 2, 1)]) == 1
    versions = _versions(spark, path)
    assert [(r.name, r._is_deleted) for r in versions] == [("x", False), ("x", True)]
    _assert_contiguous(versions)


def test_scd2_late_lower_lsn_change_between_versions(spark: SparkSession, root: str) -> None:
    path = f"{root}/silver/t"
    _merge(spark, path, [cdc("r", "a", "x", 1, 0), cdc("u", "a", "z", 5, 1)])
    assert _merge(spark, path, [cdc("u", "a", "y", 3, 2)]) == 1
    versions = _versions(spark, path)
    assert [(r.name, r.valid_from) for r in versions] == [
        ("x", BEGINNING_OF_TIME),
        ("y", source_ts(3)),
        ("z", source_ts(5)),
    ]
    _assert_contiguous(versions)


def test_scd2_first_write_with_several_versions_per_key(spark: SparkSession, root: str) -> None:
    path = f"{root}/silver/t"
    batch = [
        cdc("r", "a", "x", 1, 0),
        cdc("u", "a", "y", 2, 1),
        cdc("u", "a", "y", 3, 2, updated=7),
        cdc("u", "a", "z", 4, 3),
    ]
    assert _merge(spark, path, batch) == 3
    versions = _versions(spark, path)
    assert [(r.name, r._source_lsn) for r in versions] == [("x", 1), ("y", 2), ("z", 4)]
    assert versions[0].valid_from == BEGINNING_OF_TIME
    _assert_contiguous(versions)


def test_scd2_first_ever_create_starts_at_source_ts(spark: SparkSession, root: str) -> None:
    path = f"{root}/silver/t"
    _merge(spark, path, [cdc("r", "a", "x", 1, 0)])
    assert _merge(spark, path, [cdc("c", "b", "q", 2, 1)]) == 1
    created = [r for r in _versions(spark, path) if r.id == "b"]
    assert [(r.valid_from, r.valid_to, r.is_current) for r in created] == [
        (source_ts(2), None, True)
    ]
