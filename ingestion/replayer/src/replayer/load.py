import csv
from pathlib import Path
from typing import BinaryIO

import psycopg
from psycopg import sql

from replayer.schema import TABLES, Table, apply_schema

CHUNK_SIZE = 1024 * 1024
UTF8_BOM = b"\xef\xbb\xbf"


def strip_bom(chunk: bytes) -> bytes:
    return chunk.removeprefix(UTF8_BOM)


def read_header(handle: BinaryIO) -> list[str]:
    line = strip_bom(handle.readline()).decode("utf-8")
    handle.seek(0)
    return next(csv.reader([line]), [])


def copy_csv(conn: psycopg.Connection, table: Table, path: Path) -> int:
    statement = sql.SQL(
        "COPY {table} ({columns}) FROM STDIN WITH (FORMAT csv, HEADER true)"
    ).format(
        table=sql.Identifier("olist", table.name),
        columns=sql.SQL(", ").join(sql.Identifier(c) for c in table.columns),
    )
    with conn.cursor() as cur, path.open("rb") as handle:
        found = read_header(handle)
        if tuple(found) != table.columns:
            raise ValueError(
                f"{path.name}: header {found} does not match expected {list(table.columns)}"
            )
        with cur.copy(statement) as copy:
            first = True
            while chunk := handle.read(CHUNK_SIZE):
                copy.write(strip_bom(chunk) if first else chunk)
                first = False
        return cur.rowcount


def seed(dsn: str, data_dir: Path) -> dict[str, int]:
    missing = [t.csv_file for t in TABLES if not (data_dir / t.csv_file).is_file()]
    if missing:
        raise FileNotFoundError(f"missing CSV files in {data_dir}: {', '.join(missing)}")
    with psycopg.connect(dsn) as conn:
        apply_schema(conn)
        targets = sql.SQL(", ").join(sql.Identifier("olist", t.name) for t in TABLES)
        conn.execute(sql.SQL("TRUNCATE {targets} RESTART IDENTITY").format(targets=targets))
        return {t.name: copy_csv(conn, t, data_dir / t.csv_file) for t in TABLES}


def row_counts(dsn: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    with psycopg.connect(dsn) as conn:
        for t in TABLES:
            query = sql.SQL("SELECT count(*) FROM {table}").format(
                table=sql.Identifier("olist", t.name)
            )
            row = conn.execute(query).fetchone()
            assert row is not None
            counts[t.name] = row[0]
    return counts
