from pathlib import Path

import psycopg
from psycopg import sql

from replayer.schema import TABLES, Table, apply_schema

CHUNK_SIZE = 1024 * 1024
UTF8_BOM = b"\xef\xbb\xbf"


def copy_csv(conn: psycopg.Connection, table: Table, path: Path) -> int:
    statement = sql.SQL(
        "COPY {table} ({columns}) FROM STDIN WITH (FORMAT csv, HEADER true)"
    ).format(
        table=sql.Identifier("olist", table.name),
        columns=sql.SQL(", ").join(sql.Identifier(c) for c in table.columns),
    )
    with conn.cursor() as cur, path.open("rb") as handle:
        with cur.copy(statement) as copy:
            first = True
            while chunk := handle.read(CHUNK_SIZE):
                if first and chunk.startswith(UTF8_BOM):
                    chunk = chunk[len(UTF8_BOM) :]
                first = False
                copy.write(chunk)
        return cur.rowcount


def seed(dsn: str, data_dir: Path) -> dict[str, int]:
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
