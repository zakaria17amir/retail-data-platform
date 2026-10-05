"""Delta writer for `silver/product_enriched` and its rejects."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

from genai.enrichment.sources import storage_options

ENRICHED = "silver/product_enriched"
REJECTS = "silver/_rejects/product_enriched"
TS = pa.timestamp("us", tz="UTC")
ENRICHED_SCHEMA = pa.schema(
    [
        ("product_id", pa.string()),
        ("title", pa.string()),
        ("description", pa.string()),
        ("tags", pa.list_(pa.string())),
        ("language", pa.string()),
        ("model", pa.string()),
        ("prompt_hash", pa.string()),
        ("enriched_at", TS),
    ]
)
REJECTS_SCHEMA = pa.schema(
    [
        ("product_id", pa.string()),
        ("prompt_hash", pa.string()),
        ("model", pa.string()),
        ("rule_id", pa.string()),
        ("reason", pa.string()),
        ("record_json", pa.string()),
        ("rejected_at", TS),
    ]
)


def done_keys(root: str) -> set[tuple[str, str]]:
    """(product_id, prompt_hash) already enriched or rejected."""
    keys: set[tuple[str, str]] = set()
    for path in (ENRICHED, REJECTS):
        try:
            table = DeltaTable(f"{root}/{path}", storage_options=storage_options(root))
        except TableNotFoundError:
            continue
        data = table.to_pyarrow_table(columns=["product_id", "prompt_hash"])
        keys.update(
            zip(data["product_id"].to_pylist(), data["prompt_hash"].to_pylist(), strict=True)
        )
    return keys


def append(root: str, path: str, rows: Sequence[dict[str, Any]], schema: Any) -> None:
    if rows:
        write_deltalake(
            f"{root}/{path}",
            pa.Table.from_pylist(list(rows), schema=schema),
            mode="append",
            storage_options=storage_options(root),
        )
