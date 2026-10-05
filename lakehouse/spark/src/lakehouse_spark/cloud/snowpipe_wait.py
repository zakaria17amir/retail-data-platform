"""Wait until Snowpipe has loaded one export run, as listed in its manifest; exit 1 on timeout.

Runs in the pinned dbt-snowflake tool env (its snowflake-connector-python), not on Spark:
    python -m lakehouse_spark.cloud.snowpipe_wait <manifest.json> [--timeout S] [--interval S]
Snowpipe loads a file atomically, so a table is done when its distinct _EXPORT_FILE count for the
run equals the manifest's parts and the row count matches. A 0-row export is one schema-only part
that loads no rows, so there is nothing to wait for. Connects like the dbt `snowflake` profile.
"""

import argparse
import json
import logging
import os
import sys
import time
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from typing import Any

Loaded = dict[str, tuple[int, int]]
log = logging.getLogger("snowpipe_wait")


def snowflake_table(table: str) -> str:
    return table.rsplit("/", 1)[-1].lstrip("_").upper()


def status_query(database: str, tables: Iterable[str]) -> str:
    return "\nunion all\n".join(
        f"select '{t}', count(distinct _export_file), count(*) "
        f"from {database}.SILVER.{snowflake_table(t)} "
        "where split_part(_export_file, '/', -2) = %(run_id)s"
        for t in tables
    )


def pending(manifest: Mapping[str, Any], loaded: Loaded) -> list[str]:
    left = []
    for table, want in manifest["tables"].items():
        files, rows = loaded.get(table, (0, 0))
        parts = want["parts"] if want["rows"] else 0
        if (files, rows) != (parts, want["rows"]):
            left.append(f"{table}: files {files}/{parts}, rows {rows}/{want['rows']}")
    return left


def wait(
    manifest: Mapping[str, Any],
    fetch: Callable[[], Loaded],
    timeout: float,
    interval: float,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    deadline = clock() + timeout
    while left := pending(manifest, fetch()):
        if clock() >= deadline:
            raise TimeoutError("; ".join(left))
        log.info("waiting: %s", "; ".join(left))
        sleep(interval)


def connect_args(env: Mapping[str, str]) -> dict[str, str]:
    args = {
        "account": env.get("SNOWFLAKE_ACCOUNT", ""),
        "user": env.get("SNOWFLAKE_USER") or "DBT_SERVICE",
        "role": env.get("SNOWFLAKE_ROLE") or "TRANSFORMER",
        "warehouse": env.get("SNOWFLAKE_WAREHOUSE") or "RETAIL_WH",
        "database": env.get("SNOWFLAKE_DATABASE") or "RETAIL",
    }
    if key := env.get("SNOWFLAKE_PRIVATE_KEY"):
        # the connector takes an unencrypted key as base64 DER: the PEM body without its armour
        args["private_key"] = "".join(
            line.strip() for line in key.splitlines() if not line.startswith("-----")
        )
    else:
        args["private_key_file"] = env.get("SNOWFLAKE_PRIVATE_KEY_PATH", "")
        if pwd := env.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"):
            args["private_key_file_pwd"] = pwd
    return args


def fetcher(manifest: Mapping[str, Any], env: Mapping[str, str]) -> Callable[[], Loaded]:
    import snowflake.connector  # type: ignore[import-not-found]  # only in the dbt-snowflake env

    args = connect_args(env)
    cur = snowflake.connector.connect(**args).cursor()
    sql = status_query(args["database"], manifest["tables"])
    params = {"run_id": manifest["run_id"]}
    return lambda: {t: (int(f), int(r)) for t, f, r in cur.execute(sql, params).fetchall()}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="snowpipe-wait")
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--timeout", type=float, default=900)
    parser.add_argument("--interval", type=float, default=20)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    manifest = json.loads(args.manifest.read_text())
    try:
        wait(manifest, fetcher(manifest, os.environ), args.timeout, args.interval)
    except TimeoutError as exc:
        print(f"Snowpipe did not load run {manifest['run_id']}: {exc}", file=sys.stderr)
        return 1
    log.info("run %s loaded: %s tables", manifest["run_id"], len(manifest["tables"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
