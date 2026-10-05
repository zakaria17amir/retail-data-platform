"""EMR Serverless entrypoint: `silver` or `export` against LAKEHOUSE_URI with a caller-given run id.

On EMR, LAKEHOUSE_URI is s3://<lakehouse bucket> and S3 goes through EMR's S3A connector with the
job role, so no MinIO endpoint or keys are set. The Delta jars come from the release
(/usr/share/aws/delta/lib via spark.jars); their bundled Python module is added before the jobs are
imported. Quality (Great Expectations, Python 3.12 + pandas) is not shipped here: it runs locally
and from Airflow only.
"""

import argparse
import glob
import logging
import os
import re
from collections.abc import Mapping

from pyspark.sql import SparkSession

from lakehouse_spark.cli import lakehouse_uri

COMMANDS = ("silver", "export")
RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")
DELTA_CONF: dict[str, bool | float | int | str | None] = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.session.timeZone": "UTC",
}
EMR_DELTA_JARS = "/usr/share/aws/delta/lib/delta-spark*.jar"
log = logging.getLogger("cloud")


def _run_id(value: str) -> str:
    if not RUN_ID.fullmatch(value):
        raise argparse.ArgumentTypeError(f"run id must match {RUN_ID.pattern}: {value!r}")
    return value


def parse_args(argv: list[str] | None, env: Mapping[str, str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="lakehouse-cloud")
    parser.add_argument("command", choices=COMMANDS)
    parser.add_argument("--run-id", required=True, type=_run_id)
    parser.add_argument("--root", default=lakehouse_uri(env), help="default LAKEHOUSE_URI")
    args = parser.parse_args(argv)
    args.root = args.root.rstrip("/")
    return args


def build_session(command: str) -> SparkSession:
    spark = SparkSession.builder.appName(f"cloud-{command}").config(map=DELTA_CONF).getOrCreate()
    try:
        import delta  # noqa: F401
    except ImportError:
        for jar in glob.glob(EMR_DELTA_JARS)[:1]:
            spark.sparkContext.addPyFile(jar)
    return spark


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv, os.environ)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    spark = build_session(args.command)
    log.info("%s run %s on %s", args.command, args.run_id, args.root)
    if args.command == "silver":
        from lakehouse_spark.silver import job
        from lakehouse_spark.silver.tables import TABLES

        for spec in TABLES:
            job.run_table(spark, spec, args.root, args.run_id)
    else:
        from lakehouse_spark.cloud.export import export_silver

        export_silver(spark, args.root, args.run_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
