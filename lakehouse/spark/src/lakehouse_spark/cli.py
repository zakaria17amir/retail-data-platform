import argparse
import json
import os
from collections import Counter
from collections.abc import Mapping
from typing import Any

OLIST_TABLES = (
    "customers",
    "geolocation",
    "order_items",
    "order_payments",
    "order_reviews",
    "orders",
    "product_category_name_translation",
    "products",
    "sellers",
)
EVENT_TYPES = ("add_to_cart", "checkout_started", "page_view", "product_view", "search")
QUARANTINE_SOURCES = ("events", "olist")


def storage_options(root: str, env: Mapping[str, str]) -> dict[str, str] | None:
    if not root.startswith("s3"):
        return None
    endpoint = env.get("MINIO_ENDPOINT") or f"http://127.0.0.1:{env.get('MINIO_PORT', '9000')}"
    return {
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_ACCESS_KEY_ID": env.get("MINIO_ROOT_USER", "minio"),
        "AWS_SECRET_ACCESS_KEY": env.get("MINIO_ROOT_PASSWORD", "minio12345"),
        "AWS_ALLOW_HTTP": "true",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


def _open(path: str, options: dict[str, str] | None) -> Any:
    from deltalake import DeltaTable
    from deltalake.exceptions import TableNotFoundError

    try:
        return DeltaTable(path, storage_options=options)
    except TableNotFoundError:
        return None


def status_counts(root: str, env: Mapping[str, str]) -> dict[str, Any]:
    options = storage_options(root, env)
    paths = {f"bronze/olist/{t}": f"{root}/bronze/olist/{t}" for t in OLIST_TABLES}
    paths |= {f"bronze/events/{t}": f"{root}/bronze/events/{t}" for t in EVENT_TYPES}
    paths |= {f"quarantine/{s}": f"{root}/bronze/_quarantine/{s}" for s in QUARANTINE_SOURCES}
    result: dict[str, Any] = {}
    for name, path in paths.items():
        table = _open(path, options)
        result[name] = 0 if table is None else table.to_pyarrow_dataset().count_rows()
        if name.startswith("quarantine/") and result[name]:
            reasons = table.to_pyarrow_table(columns=["reason"]).column("reason").to_pylist()
            result[f"{name} reasons"] = dict(sorted(Counter(reasons).items()))
    return dict(sorted(result.items()))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="bronze")
    commands = parser.add_subparsers(dest="command", required=True)
    status = commands.add_parser("status", help="row counts of the bronze Delta tables")
    status.add_argument("--root", default=f"s3://{os.environ.get('LAKEHOUSE_BUCKET', 'lakehouse')}")
    status.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args(argv)

    counts = status_counts(args.root.rstrip("/"), os.environ)
    if args.as_json:
        print(json.dumps(counts))
    else:
        for name, value in counts.items():
            print(f"{name}: {json.dumps(value) if isinstance(value, dict) else value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
