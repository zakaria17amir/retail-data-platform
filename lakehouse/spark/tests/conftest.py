import os
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# pyspark collects timestamps in the process zone; pin it so expected datetimes are UTC everywhere
os.environ["TZ"] = "UTC"
getattr(time, "tzset", lambda: None)()  # POSIX only; the spark tests run in the Linux container


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("bronze-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def root(tmp_path: Path) -> str:
    return tmp_path.as_uri()
