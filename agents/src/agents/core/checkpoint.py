import os
from collections.abc import Iterator
from contextlib import contextmanager

import psycopg
from langgraph.checkpoint.postgres import PostgresSaver
from psycopg.conninfo import make_conninfo
from psycopg.rows import DictRow, dict_row

SCHEMA = "agents"


@contextmanager
def postgres_checkpointer(dsn: str | None = None) -> Iterator[PostgresSaver]:
    """LangGraph checkpointer whose tables live in the `agents` schema (search_path)."""
    dsn = dsn or os.environ["POSTGRES_DSN"]
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
    conninfo = make_conninfo(dsn, options=f"-c search_path={SCHEMA}")
    conn: psycopg.Connection[DictRow]
    with psycopg.connect(
        conninfo, autocommit=True, prepare_threshold=0, row_factory=dict_row
    ) as conn:
        saver = PostgresSaver(conn)
        saver.setup()
        yield saver
