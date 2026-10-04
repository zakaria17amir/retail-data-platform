import json
import os
from collections.abc import Mapping, Sequence

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError


def storage_options(root: str, env: Mapping[str, str] = os.environ) -> dict[str, str]:
    if not root.startswith("s3"):
        return {}
    return {
        "AWS_ENDPOINT_URL": env.get("MINIO_ENDPOINT") or "http://127.0.0.1:9000",
        "AWS_ACCESS_KEY_ID": env.get("MINIO_ROOT_USER", "minio"),
        "AWS_SECRET_ACCESS_KEY": env.get("MINIO_ROOT_PASSWORD", "minio12345"),
        "AWS_ALLOW_HTTP": "true",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


def _open(root: str, path: str) -> DeltaTable:
    return DeltaTable(f"{root}/{path}", storage_options=storage_options(root))


def current_rows(df: pd.DataFrame) -> pd.DataFrame:
    if "is_current" in df:
        df = df[df["is_current"].eq(True)]
    if "_is_deleted" in df:
        df = df[~df["_is_deleted"].eq(True)]
    return df.reset_index(drop=True)


def read_silver(root: str, table: str, current_only: bool = True) -> pd.DataFrame:
    df = _open(root, f"silver/{table}").to_pandas()
    return current_rows(df) if current_only else df


def bronze_distinct_keys(root: str, bronze_table: str, key: Sequence[str]) -> int:
    try:
        data = _open(root, f"bronze/olist/{bronze_table}").to_pyarrow_table(
            columns=["after", "before"]
        )
    except TableNotFoundError:
        return 0
    after, before = data["after"].combine_chunks(), data["before"].combine_chunks()
    keys = pa.table(
        {k: pc.coalesce(pc.struct_field(after, k), pc.struct_field(before, k)) for k in key}
    )
    return int(keys.drop_null().group_by(list(key)).aggregate([]).num_rows)


def rejected_distinct_keys(root: str, table: str, key: Sequence[str]) -> int:
    try:
        records = _open(root, f"silver/_rejects/{table}").to_pyarrow_table(columns=["record_json"])
    except TableNotFoundError:
        return 0
    rows = (json.loads(raw) for raw in records["record_json"].to_pylist())
    keys = (tuple(row.get(k) for k in key) for row in rows)
    return len({tuple(map(str, k)) for k in keys if None not in k})
