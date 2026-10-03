import hashlib
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl

from replayer.schema import TABLES

REFERENCE_TABLES: tuple[str, ...] = (
    "product_category_name_translation",
    "products",
    "sellers",
    "geolocation",
)

KEY_COLUMNS: dict[str, tuple[str, ...]] = {
    "customers": ("customer_id",),
    "orders": ("order_id",),
    "order_items": ("order_id", "order_item_id"),
    "order_payments": ("order_id", "payment_sequential"),
    "order_reviews": ("order_id", "review_id"),
}

ID_COLUMNS = frozenset({"order_id", "customer_id", "customer_unique_id", "review_id"})
TIMESTAMP_SUFFIXES = ("_timestamp", "_date", "_at")
TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

Pairs = tuple[tuple[str, object], ...]


@dataclass(frozen=True, order=True, slots=True)
class Change:
    ts: datetime
    seq: int
    table: str
    key: Pairs
    values: Pairs


def _columns(table: str) -> tuple[str, ...]:
    return next(t.columns for t in TABLES if t.name == table)


def _read(data_dir: Path, table: str) -> list[dict[str, str | None]]:
    csv_file = next(t.csv_file for t in TABLES if t.name == table)
    return list(pl.read_csv(data_dir / csv_file, infer_schema=False).iter_rows(named=True))


def _parse(value: str | None) -> datetime | None:
    return datetime.strptime(value, TIME_FORMAT) if value else None


def _convert(column: str, value: str | None) -> object:
    if not value:
        return None
    if column.endswith(TIMESTAMP_SUFFIXES):
        return datetime.strptime(value, TIME_FORMAT)
    return value


def _pairs(row: dict[str, str | None], columns: tuple[str, ...]) -> Pairs:
    return tuple(sorted((c, _convert(c, row[c])) for c in columns))


def _split(table: str, row: dict[str, str | None]) -> tuple[Pairs, Pairs]:
    keys = KEY_COLUMNS[table]
    key = tuple(sorted((c, row[c]) for c in keys))
    values = _pairs(row, tuple(c for c in _columns(table) if c not in keys))
    return key, values


def _group(rows: list[dict[str, str | None]]) -> dict[str | None, list[dict[str, str | None]]]:
    grouped: dict[str | None, list[dict[str, str | None]]] = defaultdict(list)
    for row in rows:
        grouped[row["order_id"]].append(row)
    return grouped


def build_timeline(data_dir: Path, start: datetime | None, until: datetime | None) -> list[Change]:
    customers = {r["customer_id"]: r for r in _read(data_dir, "customers")}
    items = _group(_read(data_dir, "order_items"))
    payments = _group(_read(data_dir, "order_payments"))
    reviews = _group(_read(data_dir, "order_reviews"))

    changes: list[Change] = []

    def emit(ts: datetime, table: str, key: Pairs, values: Pairs) -> None:
        changes.append(Change(ts, len(changes), table, key, values))

    for order in _read(data_dir, "orders"):
        purchase = _parse(order["order_purchase_timestamp"])
        if purchase is None or (start and purchase < start) or (until and purchase >= until):
            continue
        order_id = order["order_id"]
        key = (("order_id", order_id),)

        customer = customers.get(order["customer_id"])
        if customer is not None:
            emit(purchase, "customers", *_split("customers", customer))
        emit(
            purchase,
            "orders",
            key,
            tuple(
                sorted(
                    [
                        ("customer_id", order["customer_id"]),
                        ("order_status", "created"),
                        ("order_approved_at", None),
                        ("order_delivered_carrier_date", None),
                        ("order_delivered_customer_date", None),
                        ("order_purchase_timestamp", purchase),
                        (
                            "order_estimated_delivery_date",
                            _parse(order["order_estimated_delivery_date"]),
                        ),
                    ]
                )
            ),
        )
        for item in items.get(order_id, []):
            emit(purchase, "order_items", *_split("order_items", item))

        last = purchase
        last_status = "created"
        approved = _parse(order["order_approved_at"])
        steps = (
            ("approved", "order_approved_at", approved),
            (
                "shipped",
                "order_delivered_carrier_date",
                _parse(order["order_delivered_carrier_date"]),
            ),
            (
                "delivered",
                "order_delivered_customer_date",
                _parse(order["order_delivered_customer_date"]),
            ),
        )
        payments_emitted = False
        for status, column, ts in steps:
            if ts is None:
                continue
            last = max(last, ts)
            emit(last, "orders", key, ((column, ts), ("order_status", status)))
            last_status = status
            if status == "approved":
                for payment in payments.get(order_id, []):
                    emit(last, "order_payments", *_split("order_payments", payment))
                payments_emitted = True
        if not payments_emitted:
            for payment in payments.get(order_id, []):
                emit(purchase, "order_payments", *_split("order_payments", payment))
        if order["order_status"] != last_status:
            emit(last, "orders", key, (("order_status", order["order_status"]),))

        for review in reviews.get(order_id, []):
            created = _parse(review["review_creation_date"]) or purchase
            review_key, review_values = _split("order_reviews", review)
            answered = _parse(review["review_answer_timestamp"])
            initial = tuple(
                (c, None if c == "review_answer_timestamp" else v) for c, v in review_values
            )
            emit(created, "order_reviews", review_key, initial)
            if answered is not None:
                emit(
                    max(created, answered),
                    "order_reviews",
                    review_key,
                    (("review_answer_timestamp", answered),),
                )

    return sorted(changes)


def _new_id(old: object, iteration: int) -> str:
    return hashlib.sha1(f"{old}:{iteration}".encode()).hexdigest()[:32]


def _shift_pairs(pairs: Pairs, span: timedelta, iteration: int) -> Pairs:
    shifted: list[tuple[str, object]] = []
    for column, value in pairs:
        if column in ID_COLUMNS and value is not None:
            value = _new_id(value, iteration)
        elif isinstance(value, datetime):
            value = value + span * iteration
        shifted.append((column, value))
    return tuple(shifted)


def shift_history(changes: list[Change], span: timedelta, iteration: int) -> list[Change]:
    return [
        replace(
            change,
            ts=change.ts + span * iteration,
            key=_shift_pairs(change.key, span, iteration),
            values=_shift_pairs(change.values, span, iteration),
        )
        for change in changes
    ]
