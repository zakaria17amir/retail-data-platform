import argparse
import os
import uuid
from datetime import UTC, datetime

import pyarrow as pa
from deltalake import write_deltalake

from lakehouse_quality.io import storage_options
from lakehouse_quality.suites import run_checks

DQ_SCHEMA = pa.schema(
    [
        ("run_id", pa.string()),
        ("table", pa.string()),
        ("expectation", pa.string()),
        ("severity", pa.string()),
        ("success", pa.bool_()),
        ("observed_value", pa.string()),
        ("details", pa.string()),
        ("checked_at", pa.timestamp("us", tz="UTC")),
    ]
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="quality")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="validate silver and append _dq_results")
    run.add_argument("--root", default=f"s3://{os.environ.get('LAKEHOUSE_BUCKET', 'lakehouse')}")
    root = parser.parse_args(argv).root.rstrip("/")

    now = datetime.now(UTC)
    checks = run_checks(root, now)
    run_id = uuid.uuid4().hex
    rows = [
        {
            "run_id": run_id,
            "table": c.table,
            "expectation": c.expectation,
            "severity": c.severity,
            "success": c.success,
            "observed_value": c.observed,
            "details": c.details,
            "checked_at": now,
        }
        for c in checks
    ]
    write_deltalake(
        f"{root}/silver/_dq_results",
        pa.Table.from_pylist(rows, schema=DQ_SCHEMA),
        mode="append",
        storage_options=storage_options(root),
    )
    for c in checks:
        status = "PASS" if c.success else "FAIL"
        print(f"{status} {c.severity} {c.table} {c.expectation} observed={c.observed} {c.details}")
    return int(any(c.severity == "critical" and not c.success for c in checks))


if __name__ == "__main__":
    raise SystemExit(main())
