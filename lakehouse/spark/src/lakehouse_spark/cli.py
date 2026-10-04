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
SILVER_TABLES = (
    "catalog/categories",
    "catalog/products",
    "party/customers",
    "party/sellers",
    "geo/geolocation_points",
    "geo/zip_centroids",
    "sales/orders",
    "sales/order_items",
    "sales/order_payments",
    "sales/order_reviews",
    "events/clickstream",
)


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


def _row_count(table: Any) -> int:
    import pyarrow as pa

    records = pa.table(table.get_add_actions(flatten=True)).column("num_records").to_pylist()
    if None in records:
        return int(table.to_pyarrow_dataset().count_rows())
    return int(sum(records))


def _current_count(table: Any) -> int:
    if "is_current" not in [f.name for f in table.schema().fields]:
        return _row_count(table)
    flags = table.to_pyarrow_table(columns=["is_current"]).column("is_current").to_pylist()
    return sum(1 for flag in flags if flag)


def rule_lines(root: str, env: Mapping[str, str]) -> list[str]:
    table = _open(f"{root}/silver/_rule_metrics", storage_options(root, env))
    if table is None:
        return []
    columns = ["run_id", "table", "rule_id", "rows_in", "rows_rejected", "run_ts"]
    rows = table.to_pyarrow_table(columns=columns).to_pylist()
    if not rows:
        return []
    latest = max(rows, key=lambda r: r["run_ts"])["run_id"]
    totals: dict[tuple[str, str], list[int]] = {}
    for r in rows:
        if r["run_id"] == latest:
            total = totals.setdefault((r["table"], r["rule_id"]), [0, 0])
            total[0] += r["rows_in"]
            total[1] += r["rows_rejected"]
    return [
        f"rule {rule} rejected {rejected} rows ({100 * rejected / rows_in:.3f} %) in {name}"
        for (name, rule), (rows_in, rejected) in sorted(totals.items())
        if rejected
    ]


def status_counts(root: str, env: Mapping[str, str]) -> dict[str, Any]:
    options = storage_options(root, env)
    paths = {f"bronze/olist/{t}": f"{root}/bronze/olist/{t}" for t in OLIST_TABLES}
    paths |= {f"bronze/events/{t}": f"{root}/bronze/events/{t}" for t in EVENT_TYPES}
    paths |= {f"quarantine/{s}": f"{root}/bronze/_quarantine/{s}" for s in QUARANTINE_SOURCES}
    result: dict[str, Any] = {}
    for name, path in paths.items():
        table = _open(path, options)
        result[name] = 0 if table is None else _row_count(table)
        if name.startswith("quarantine/") and result[name]:
            reasons = table.to_pyarrow_table(columns=["reason"]).column("reason").to_pylist()
            result[f"{name} reasons"] = dict(sorted(Counter(reasons).items()))
    for name in SILVER_TABLES:
        table = _open(f"{root}/silver/{name}", options)
        result[f"silver/{name}"] = 0 if table is None else _current_count(table)
        rejects = _open(f"{root}/silver/_rejects/{name}", options)
        if rejects is not None:
            result[f"silver/_rejects/{name}"] = _row_count(rejects)
    return dict(sorted(result.items()))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="bronze")
    commands = parser.add_subparsers(dest="command", required=True)
    status = commands.add_parser("status", help="row counts of the bronze and silver Delta tables")
    status.add_argument("--root", default=f"s3://{os.environ.get('LAKEHOUSE_BUCKET', 'lakehouse')}")
    status.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args(argv)

    root = args.root.rstrip("/")
    counts, rules = status_counts(root, os.environ), rule_lines(root, os.environ)
    if args.as_json:
        print(json.dumps({**counts, "rules": rules}))
    else:
        for name, value in counts.items():
            print(f"{name}: {json.dumps(value) if isinstance(value, dict) else value}")
        for line in rules:
            print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
