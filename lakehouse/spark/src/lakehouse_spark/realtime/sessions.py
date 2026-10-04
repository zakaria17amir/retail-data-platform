"""Session features with session_window gap semantics in update mode.

Spark rejects session_window aggregations in update output mode, so sessions are kept in
applyInPandasWithState: state holds the events of a session_id's still-open sessions (dataset
time), an event one gap or more after the previous one opens a new session, and an event-time
timeout drops the state once the watermark passes the last event plus the gap.
"""

from collections.abc import Iterable, Iterator
from typing import Any

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# recommendation_shown/clicked are system feedback, not shopper activity: not session events
SESSION_TYPES = ("add_to_cart", "checkout_started", "page_view", "product_view", "search")
GAP_MS = 30 * 60 * 1000
WATERMARK = "2 hours"
STATE_FIELDS = ("ts_ms", "event_type", "product_id", "category")
STATE_SCHEMA = StructType(
    [StructField("ts_ms", ArrayType(LongType()))]
    + [StructField(c, ArrayType(StringType())) for c in STATE_FIELDS[1:]]
)
OUTPUT_SCHEMA = StructType(
    [
        StructField("session_id", StringType()),
        StructField("n_events", LongType()),
        StructField("n_product_views", LongType()),
        StructField("n_categories", LongType()),
        StructField("last_category", StringType()),
        StructField("last_product_ids", StringType()),
        StructField("n_cart_adds", LongType()),
        StructField("dwell_seconds", LongType()),
        StructField("session_start_ts", TimestampType()),
        StructField("event_ts", TimestampType()),
    ]
)
LAST_PRODUCTS = 5


def _features(session_id: str, events: pd.DataFrame) -> dict[str, Any]:
    start, end = int(events["ts_ms"].iloc[0]), int(events["ts_ms"].iloc[-1])
    categories = events["category"].dropna()
    products = events["product_id"].dropna().iloc[::-1].drop_duplicates().head(LAST_PRODUCTS)
    return {
        "session_id": session_id,
        "n_events": len(events),
        "n_product_views": int((events["event_type"] == "product_view").sum()),
        "n_categories": int(categories.nunique()),
        "last_category": categories.iloc[-1] if len(categories) else None,
        "last_product_ids": ",".join(products) if len(products) else None,
        "n_cart_adds": int((events["event_type"] == "add_to_cart").sum()),
        "dwell_seconds": (end - start) // 1000,
        "session_start_ts": pd.Timestamp(start, unit="ms"),
        # + n_events ms: every update adds events, so the push is always newer than the stored row
        # even when out-of-order events (one bronze table per type) leave the last event time as is
        "event_ts": pd.Timestamp(end + len(events), unit="ms"),
    }


def update_sessions(
    key: tuple[str], batches: Iterable[pd.DataFrame], state: GroupState
) -> Iterator[pd.DataFrame]:
    if state.hasTimedOut:
        state.remove()
        return
    new = pd.concat(list(batches), ignore_index=True)
    new["ts_ms"] = (new["event_ts"] - pd.Timestamp(0)) // pd.Timedelta(milliseconds=1)
    events = new[list(STATE_FIELDS)].assign(_new=True)
    if state.exists:
        old = pd.DataFrame(dict(zip(STATE_FIELDS, state.get, strict=True))).assign(_new=False)
        events = pd.concat([old, events], ignore_index=True)
    events = events.sort_values("ts_ms", kind="stable")
    session_no = (events["ts_ms"].diff() >= GAP_MS).cumsum()
    watermark = state.getCurrentWatermarkMs()
    rows: list[dict[str, Any]] = []
    still_open: list[pd.DataFrame] = []
    for _, session in events.groupby(session_no):
        if session["_new"].any():
            rows.append(_features(key[0], session))
        if int(session["ts_ms"].iloc[-1]) + GAP_MS > watermark:
            still_open.append(session)
    if still_open:
        kept = pd.concat(still_open)
        state.update(tuple(kept[c].tolist() for c in STATE_FIELDS))
        state.setTimeoutTimestamp(int(kept["ts_ms"].iloc[-1]) + GAP_MS)
    else:
        state.remove()
    yield pd.DataFrame(rows, columns=OUTPUT_SCHEMA.fieldNames())


def session_features(events: DataFrame) -> DataFrame:
    """events: event_type, session_id, product_id, category, event_ts (dataset time)."""
    return (
        events.filter(F.col("session_id").isNotNull())
        .withWatermark("event_ts", WATERMARK)
        .groupBy("session_id")
        .applyInPandasWithState(
            update_sessions,
            OUTPUT_SCHEMA,
            STATE_SCHEMA,
            "update",
            GroupStateTimeout.EventTimeTimeout,
        )
    )
