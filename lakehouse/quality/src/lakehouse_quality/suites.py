import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Literal

import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd
from deltalake.exceptions import TableNotFoundError
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
    ProgressBarsConfig,
)

from lakehouse_quality.io import (
    bronze_distinct_keys,
    current_rows,
    read_silver,
    rejected_distinct_keys,
)

Severity = Literal["critical", "warning"]
META = ("_source_lsn", "_source_ts", "_is_deleted", "_silver_loaded_at", "_run_id")
SCD2 = ("valid_from", "valid_to", "is_current")


@dataclass(frozen=True)
class Check:
    table: str
    expectation: str
    severity: Severity
    success: bool
    observed: str
    details: str


@dataclass(frozen=True)
class TableSpec:
    key: tuple[str, ...]
    columns: tuple[str, ...]
    bronze: str | None = None
    scd2: bool = False


def _ts(*names: str) -> tuple[str, ...]:
    return tuple(f"{n}_{suffix}" for n in names for suffix in ("local", "utc"))


def _cdc(key: tuple[str, ...], cols: tuple[str, ...], bronze: str, scd2: bool = False) -> TableSpec:
    return TableSpec(key, key + cols + META + (SCD2 if scd2 else ()), bronze, scd2)


TABLES: dict[str, TableSpec] = {
    "catalog/categories": _cdc(
        ("product_category_name",),
        ("product_category_name_english",),
        "product_category_name_translation",
    ),
    "catalog/products": _cdc(
        ("product_id",),
        (
            "product_category_name",
            "product_category_name_english",
            "product_name_length",
            "product_description_length",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ),
        "products",
        scd2=True,
    ),
    "party/customers": _cdc(
        ("customer_id",),
        ("customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"),
        "customers",
        scd2=True,
    ),
    "party/sellers": _cdc(
        ("seller_id",), ("seller_zip_code_prefix", "seller_city", "seller_state"), "sellers", True
    ),
    "geo/geolocation_points": _cdc(
        ("geolocation_pk",), ("zip_code_prefix", "lat", "lng", "city", "state"), "geolocation"
    ),
    "geo/zip_centroids": TableSpec(
        ("zip_code_prefix",), ("zip_code_prefix", "lat", "lng", "n_points", "state")
    ),
    "sales/orders": _cdc(
        ("order_id",),
        ("customer_id", "order_status")
        + _ts(
            "order_purchase_ts",
            "order_approved_ts",
            "order_delivered_carrier_ts",
            "order_delivered_customer_ts",
            "order_estimated_delivery_ts",
        )
        + (
            "flag_approved_before_purchase",
            "flag_carrier_before_approved",
            "flag_delivered_before_carrier",
            "flag_delivered_before_purchase",
        ),
        "orders",
    ),
    "sales/order_items": _cdc(
        ("order_id", "order_item_id"),
        ("product_id", "seller_id", *_ts("shipping_limit_ts"), "price", "freight_value"),
        "order_items",
    ),
    "sales/order_payments": _cdc(
        ("order_id", "payment_sequential"),
        ("payment_type", "payment_installments", "payment_value"),
        "order_payments",
    ),
    "sales/order_reviews": _cdc(
        ("review_pk",),
        ("review_id", "order_id", "review_score", "review_comment_title", "review_comment_message")
        + _ts("review_creation_ts", "review_answer_ts"),
        "order_reviews",
    ),
    "events/clickstream": TableSpec(
        ("event_id",),
        (
            "event_id",
            "event_type",
            "session_id",
            "customer_id",
            "device",
            "referrer",
            "event_ts_local",
            "event_ts_utc",
            "product_id",
            "search_query",
            "quantity",
            "order_id",
            "utm_campaign",
            "event_date",
            "_bronze_ingest_ts",
            "_silver_loaded_at",
            "_run_id",
            "rank",
            "rec_model_version",
            "rec_strategy",
        ),
    ),
}

REFERENCES = (
    ("sales/order_items", "order_id", "sales/orders"),
    ("sales/order_items", "product_id", "catalog/products"),
    ("sales/order_items", "seller_id", "party/sellers"),
    ("sales/order_payments", "order_id", "sales/orders"),
    ("sales/order_reviews", "order_id", "sales/orders"),
    ("sales/orders", "customer_id", "party/customers"),
)

Validate = Callable[[str, pd.DataFrame, gxe.Expectation, Severity], Check]


def gx_errors(info: dict[str, Any]) -> list[str]:
    # GX 1.x: flat {raised_exception, exception_message, ...} or nested per metric id
    shapes = [info, *(v for v in info.values() if isinstance(v, dict))]
    return [str(s.get("exception_message")) for s in shapes if s.get("raised_exception")]


def _validator() -> Validate:
    context = gx.get_context(
        mode="ephemeral",
        project_config=DataContextConfig(
            analytics_enabled=False,
            progress_bars=ProgressBarsConfig(globally=False),
            store_backend_defaults=InMemoryStoreBackendDefaults(),
        ),
    )
    asset = context.data_sources.add_pandas("silver").add_dataframe_asset("table")
    batches = asset.add_batch_definition_whole_dataframe("all")

    def validate(
        table: str, df: pd.DataFrame, expectation: gxe.Expectation, severity: Severity
    ) -> Check:
        result = batches.get_batch(batch_parameters={"dataframe": df}).validate(expectation)
        arg = getattr(expectation, "column", None) or ",".join(
            getattr(expectation, "column_list", None) or ()
        )
        name = f"{expectation.expectation_type}({arg})" if arg else expectation.expectation_type
        errors = gx_errors(result.exception_info or {})
        observed = result.result.get("unexpected_count", result.result.get("observed_value"))
        details = (
            errors or result.result.get("details") or result.result.get("partial_unexpected_list")
        )
        return Check(table, name, severity, bool(result.success), str(observed), str(details or ""))

    return validate


Outcome = tuple[bool, str, str]


def _pandas(
    table: str, name: str, severity: Severity, fn: Callable[..., Outcome], *args: object
) -> Check:
    try:
        success, observed, details = fn(*args)
    except Exception as e:
        success, observed, details = False, "error", _error(e)
    return Check(table, name, severity, success, observed, details)


def _error(e: Exception) -> str:
    return f"{type(e).__name__}: {e}"


def _rowcount(
    root: str, table: str, spec: TableSpec, rows: pd.DataFrame, tolerance: float
) -> Outcome:
    assert spec.bronze
    silver = len(rows.drop_duplicates(list(spec.key)))
    bronze = bronze_distinct_keys(root, spec.bronze, spec.key)
    rejected = rejected_distinct_keys(root, table, spec.key)
    low = bronze - rejected - bronze * tolerance
    observed = f"silver={silver} bronze={bronze} rejected={rejected}"
    return low <= silver <= bronze, observed, f"expected {low:g} <= silver <= {bronze}"


def _fresh(rows: pd.DataFrame, threshold: pd.Timestamp) -> Outcome:
    if rows.empty:
        return False, "empty", f"no rows loaded; expected _silver_loaded_at >= {threshold}"
    latest = pd.to_datetime(rows["_silver_loaded_at"], utc=True).max()
    return bool(pd.notna(latest) and latest >= threshold), str(latest), f">= {threshold}"


def _orphans(child: pd.DataFrame, column: str, parent: pd.DataFrame, key: str) -> Outcome:
    values = child[column].dropna()
    bad = values[~values.isin(parent[key])]
    return bad.empty, f"orphans={len(bad)}", str(bad.unique()[:5].tolist())


def _in_set_mostly(values: pd.Series, allowed: pd.Series, mostly: float) -> Outcome:
    values = values.dropna()
    share = float(values.isin(allowed).mean()) if len(values) else 1.0
    return share >= mostly, f"{share:.4f}", f"mostly={mostly}"


def _freight(validate: Validate, items: pd.DataFrame, orders: pd.DataFrame) -> Check:
    name = "expect_column_values_to_be_between(freight_value)"
    try:
        joined = items.merge(orders[["order_id", "order_purchase_ts_utc"]], on="order_id")
        ts = pd.to_datetime(joined["order_purchase_ts_utc"], utc=True)
        cutoff = ts.max() - timedelta(days=30)
        history = joined.loc[ts <= cutoff, "freight_value"].astype(float)
        recent = joined.loc[ts > cutoff, ["freight_value"]].astype(float)
    except Exception as e:
        return Check("sales/order_items", name, "warning", False, "error", _error(e))
    if history.empty:
        return Check("sales/order_items", name, "warning", True, "no history", "")
    p99 = float(history.quantile(0.99))
    freight = gxe.ExpectColumnValuesToBeBetween(column="freight_value", max_value=p99, mostly=0.95)
    return validate("sales/order_items", recent, freight, "warning")


def run_checks(root: str, now: datetime) -> list[Check]:
    tolerance = float(os.environ.get("DQ_ROWCOUNT_TOLERANCE", "0.001"))
    threshold = pd.Timestamp(now) - timedelta(
        hours=float(os.environ.get("DQ_FRESHNESS_HOURS", "2"))
    )
    validate = _validator()
    checks: list[Check] = []
    current: dict[str, pd.DataFrame] = {}

    for table, spec in TABLES.items():
        try:
            rows = read_silver(root, table, current_only=False)
        except TableNotFoundError:
            checks.append(Check(table, "table_exists", "critical", False, "missing", ""))
            continue
        except Exception as e:
            checks.append(Check(table, "table_readable", "critical", False, "error", _error(e)))
            continue
        current[table] = cur = current_rows(rows)
        columns = gxe.ExpectTableColumnsToMatchSet(column_set=list(spec.columns), exact_match=False)
        checks.append(validate(table, rows, columns, "critical"))
        for column in spec.key:
            not_null = gxe.ExpectColumnValuesToNotBeNull(column=column)
            checks.append(validate(table, cur, not_null, "critical"))
        unique: gxe.Expectation = (
            gxe.ExpectColumnValuesToBeUnique(column=spec.key[0])
            if len(spec.key) == 1
            else gxe.ExpectCompoundColumnsToBeUnique(column_list=list(spec.key))
        )
        checks.append(validate(table, cur, unique, "critical"))
        if spec.bronze:
            checks.append(
                _pandas(
                    table,
                    "rowcount_vs_bronze",
                    "critical",
                    _rowcount,
                    root,
                    table,
                    spec,
                    rows,
                    tolerance,
                )
            )
        if "_silver_loaded_at" in spec.columns:
            checks.append(_pandas(table, "freshness", "warning", _fresh, rows, threshold))

    for child, column, parent in REFERENCES:
        if child in current and parent in current:
            name = f"referential_integrity({column} -> {parent})"
            parent_key = TABLES[parent].key[0]
            checks.append(
                _pandas(
                    child,
                    name,
                    "critical",
                    _orphans,
                    current[child],
                    column,
                    current[parent],
                    parent_key,
                )
            )

    if "sales/order_items" in current:
        items = current["sales/order_items"]
        price = gxe.ExpectColumnValuesToBeBetween(column="price", min_value=0, strict_min=True)
        checks.append(validate("sales/order_items", items, price, "critical"))
        if "sales/orders" in current:
            checks.append(_freight(validate, items, current["sales/orders"]))

    if "sales/order_reviews" in current:
        reviews = current["sales/order_reviews"]
        score = gxe.ExpectColumnValuesToBeBetween(column="review_score", min_value=1, max_value=5)
        checks.append(validate("sales/order_reviews", reviews, score, "warning"))

    if "events/clickstream" in current and "catalog/products" in current:
        events, products = current["events/clickstream"], current["catalog/products"]
        checks.append(
            _pandas(
                "events/clickstream",
                "product_id_in_products",
                "warning",
                _in_set_mostly,
                events["product_id"],
                products["product_id"],
                0.99,
            )
        )
    return checks
