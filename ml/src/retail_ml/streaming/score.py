"""`retail-ml stream-score`: Debezium `cdc.olist.orders` transitions to `approved` → late-delivery
champion (in-process pyfunc) → Kafka `ml.late_delivery_scores`, Postgres `ml.order_risk` and Delta
`gold/ml/pred_late_delivery_rt/`.

Raw input = the training contract (`ml_late_delivery_training`) rebuilt from the after image plus a
Postgres lookup (items, primary item's product/seller, first payment, customer, zip centroids).
Known gaps at approval time: the replayer writes `order_payments` right after the approval event,
so `payment_type` / `payment_installments` are often still null (the model saw nulls in training as
missing too); seller features are Feast *online* (latest snapshot), not point-in-time as in batch.

Delivery is at least once: offsets are committed after the writes of a batch (Delta append every
STREAM_BATCH_ROWS scores or STREAM_BATCH_SECONDS). A redelivered approval is re-scored and re-sent
to Kafka/Delta (same key) but `ml.order_risk` keeps the first score (`ON CONFLICT DO NOTHING`) and
only first-time scores feed `stream_score_latency_seconds` (Review Focus 1)."""

from __future__ import annotations

import importlib
import json
import logging
import os
import signal
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

import numpy as np
import pandas as pd
from prometheus_client import CollectorRegistry, Histogram, start_http_server

from retail_ml import config
from retail_ml.data import feature_store
from retail_ml.features import SELLER_FEATURES
from retail_ml.late_delivery.train import model_inputs
from retail_ml.serving.model import (
    FEATURE_REFS,
    MODEL_NAME,
    FeatureStoreUnavailable,
    LoadedModel,
    load_champion,
)

logger = logging.getLogger(__name__)
SOURCE_TOPIC = "cdc.olist.orders"
SCORES_TOPIC = "ml.late_delivery_scores"
GROUP_ID = "stream-score"
METRICS_PORT = 8001
LOCAL_TZ = "America/Sao_Paulo"  # olist timestamps are São Paulo wall clock (silver `localise`)
OUTPUT_COLUMNS = [
    "order_id",
    "probability",
    "model_version",
    "approved_ts",
    "scored_ts",
    "source_ts_ms",
]
LATENCY_BUCKETS = (0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 3.0, 5.0, 7.5, 10.0, 15.0, 30.0, 60.0)
POLL_SECONDS = 0.5
BRAZIL_BBOX = (-33.75, 5.27, -73.99, -34.79)  # silver geo rule; centroids = mean of points inside

DETAILS_SQL = """
WITH ids AS (SELECT unnest(%(ids)s::text[]) AS order_id),
items AS (
    SELECT i.order_id, i.seller_id, i.product_id, i.price, i.freight_value,
           row_number() OVER (
               PARTITION BY i.order_id ORDER BY i.price DESC, i.seller_id, i.order_item_id
           ) AS price_rank
    FROM olist.order_items AS i JOIN ids ON i.order_id = ids.order_id
),
totals AS (
    SELECT order_id, count(*) AS n_items, count(DISTINCT seller_id) AS n_sellers,
           sum(price)::float8 AS total_price, sum(freight_value)::float8 AS total_freight
    FROM items GROUP BY order_id
)
SELECT ids.order_id, c.customer_unique_id, c.customer_state, c.customer_zip_code_prefix,
       p.seller_id, s.seller_state, s.seller_zip_code_prefix,
       coalesce(t.n_items, 0) AS n_items, coalesce(t.n_sellers, 0) AS n_sellers,
       t.total_price, t.total_freight,
       CASE WHEN pr.product_id IS NOT NULL
            THEN coalesce(tr.product_category_name_english, 'unknown') END AS product_category,
       pay.payment_type, pay.payment_installments
FROM ids
JOIN olist.orders AS o ON o.order_id = ids.order_id
LEFT JOIN olist.customers AS c ON c.customer_id = o.customer_id
LEFT JOIN totals AS t ON t.order_id = ids.order_id
LEFT JOIN items AS p ON p.order_id = ids.order_id AND p.price_rank = 1
LEFT JOIN olist.sellers AS s ON s.seller_id = p.seller_id
LEFT JOIN olist.products AS pr ON pr.product_id = p.product_id
LEFT JOIN olist.product_category_name_translation AS tr
    ON tr.product_category_name = pr.product_category_name
LEFT JOIN olist.order_payments AS pay
    ON pay.order_id = ids.order_id AND pay.payment_sequential = 1
"""
CENTROIDS_SQL = """
SELECT geolocation_zip_code_prefix, avg(geolocation_lat), avg(geolocation_lng)
FROM olist.geolocation
WHERE geolocation_lat BETWEEN %s AND %s AND geolocation_lng BETWEEN %s AND %s
GROUP BY geolocation_zip_code_prefix
"""
DETAIL_COLUMNS = [
    "customer_unique_id",
    "seller_id",
    "n_items",
    "n_sellers",
    "total_price",
    "total_freight",
    "product_category",
    "payment_type",
    "payment_installments",
    "customer_state",
    "customer_lat",
    "customer_lng",
    "seller_state",
    "seller_lat",
    "seller_lng",
]


def is_approval(envelope: dict[str, Any]) -> bool:
    """Create/update whose after image is `approved` and whose before image is not (snapshot
    reads `r` are history, not transitions)."""
    if envelope.get("op") not in ("c", "u"):
        return False
    after, before = envelope.get("after") or {}, envelope.get("before") or {}
    return after.get("order_status") == "approved" and before.get("order_status") != "approved"


def dataset_utc(value: datetime | int | None) -> pd.Timestamp | None:
    """Debezium `connect` timestamp (São Paulo wall clock labelled UTC, or raw epoch millis) →
    the contract's UTC instant."""
    if value is None:
        return None
    ts = pd.Timestamp(value, unit="ms") if isinstance(value, int) else pd.Timestamp(value)
    wall = ts.tz_localize(None) if ts.tzinfo else ts
    return wall.tz_localize(LOCAL_TZ, ambiguous=True, nonexistent="shift_forward").tz_convert(UTC)


def order_rows(afters: list[dict[str, Any]], details: dict[str, dict[str, Any]]) -> pd.DataFrame:
    """Contract rows (minus seller features) per after image; an order the lookup missed gets
    n_items/n_sellers 0 and nulls, as the dbt contract's left joins would."""
    rows = []
    for a in afters:
        d = details.get(a["order_id"], {})
        rows.append(
            {
                "order_id": a["order_id"],
                "customer_id": a.get("customer_id"),
                "order_purchase_ts_utc": dataset_utc(a.get("order_purchase_timestamp")),
                "order_approved_ts_utc": dataset_utc(a.get("order_approved_at")),
                "order_estimated_delivery_ts_utc": dataset_utc(
                    a.get("order_estimated_delivery_date")
                ),
                **{c: d.get(c) for c in DETAIL_COLUMNS},
                "n_items": d.get("n_items") or 0,
                "n_sellers": d.get("n_sellers") or 0,
            }
        )
    return pd.DataFrame(rows).astype({"seller_id": object})


def upsert_order_risk(conn: Any, rows: list[dict[str, Any]], placeholder: str = "%s") -> set[str]:
    """Insert scores keyed by order_id; existing rows keep their first score. Returns the order_ids
    inserted now (portable SQL: Postgres and SQLite >= 3.35)."""
    if not rows:
        return set()
    values = ", ".join(["(" + ", ".join([placeholder] * 6) + ")"] * len(rows))
    statement = (
        "INSERT INTO ml.order_risk"
        " (order_id, probability, model_version, scored_at, source_ts, latency_ms)"
        f" VALUES {values} ON CONFLICT (order_id) DO NOTHING RETURNING order_id"
    )
    params = [
        v
        for r in rows
        for v in (
            r["order_id"],
            r["probability"],
            r["model_version"],
            r["scored_ts"],
            r["source_ts"],
            r["latency_ms"],
        )
    ]
    return {row[0] for row in conn.execute(statement, params).fetchall()}


def kafka_record(row: dict[str, Any]) -> bytes:
    def iso(ts: Any) -> str | None:
        return None if ts is None or pd.isna(ts) else pd.Timestamp(ts).isoformat()

    return json.dumps(
        {
            "order_id": row["order_id"],
            "probability": row["probability"],
            "model_version": row["model_version"],
            "approved_ts": iso(row["approved_ts"]),
            "scored_ts": iso(row["scored_ts"]),
            "source_ts_ms": row["source_ts_ms"],
        }
    ).encode()


class Writes(Protocol):
    def upsert(self, rows: list[dict[str, Any]]) -> set[str]: ...

    def produce(self, rows: list[dict[str, Any]]) -> None: ...

    def append(self, frame: pd.DataFrame) -> None: ...

    def flush(self) -> None: ...


def latency_histogram(registry: CollectorRegistry) -> Histogram:
    return Histogram(
        "stream_score_latency_seconds",
        "Debezium source.ts_ms (Postgres commit) to order_risk.scored_at, first-time scores",
        buckets=LATENCY_BUCKETS,
        registry=registry,
    )


@dataclass
class Scorer:
    model: LoadedModel
    lookup: Callable[[list[str]], dict[str, dict[str, Any]]]
    sellers: Callable[[list[str]], pd.DataFrame]
    writes: Writes
    registry: CollectorRegistry
    now: Callable[[], datetime] = lambda: datetime.now(UTC)

    def __post_init__(self) -> None:
        self.latency = latency_histogram(self.registry)

    def process(self, envelopes: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Score the approvals among `envelopes` (first per order), upsert and produce them."""
        first_seen: dict[str, dict[str, Any]] = {}
        for e in envelopes:
            if is_approval(e):
                first_seen.setdefault(e["after"]["order_id"], e)
        kept = list(first_seen.values())
        if not kept:
            return []
        ids = [e["after"]["order_id"] for e in kept]
        orders = order_rows([e["after"] for e in kept], self.lookup(ids))
        seller = self.sellers([s or "" for s in orders["seller_id"]])[SELLER_FEATURES]
        X = model_inputs(pd.concat([orders, seller.reset_index(drop=True)], axis=1))
        p = np.asarray(self.model.model.predict(X), dtype="float64")
        scored = pd.Timestamp(self.now())
        rows = []
        for e, approved, prob in zip(kept, orders["order_approved_ts_utc"], p, strict=True):
            source_ms = int(e["source"]["ts_ms"])
            source = pd.Timestamp(source_ms, unit="ms", tz=UTC)
            rows.append(
                {
                    "order_id": e["after"]["order_id"],
                    "probability": float(prob),
                    "model_version": self.model.version,
                    "approved_ts": approved,
                    "scored_ts": scored.to_pydatetime(),
                    "source_ts_ms": source_ms,
                    "source_ts": source.to_pydatetime(),
                    "latency_ms": (scored - source).total_seconds() * 1000,
                }
            )
        first = self.writes.upsert(rows)
        for r in rows:
            if r["order_id"] in first:
                self.latency.observe(r["latency_ms"] / 1000)
        self.writes.produce(rows)
        return rows

    def flush(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        frame = pd.DataFrame(rows)[OUTPUT_COLUMNS]
        for col in ("approved_ts", "scored_ts"):
            frame[col] = pd.to_datetime(frame[col], utc=True).astype("datetime64[us, UTC]")
        self.writes.append(frame)
        self.writes.flush()


def run(
    consumer: Any,
    deserialize: Callable[[Any], dict[str, Any] | None],
    scorer: Scorer,
    batch_rows: int,
    batch_seconds: float,
    clock: Callable[[], float] = time.monotonic,
    running: Callable[[], bool] = lambda: True,
) -> None:
    """Wait for a message, drain what is already fetched (no batch-fill wait), score it at once
    (Postgres + Kafka); append to Delta and commit offsets once `batch_rows` scores or
    `batch_seconds` since the first uncommitted message have accumulated: commits follow writes."""
    buffer: list[dict[str, Any]] = []
    pending, since = 0, 0.0

    def commit() -> None:
        nonlocal buffer, pending
        scorer.flush(buffer)
        consumer.commit(asynchronous=False)
        buffer, pending = [], 0

    while running():
        first = consumer.poll(POLL_SECONDS)
        messages = []
        if first is not None:
            messages = [first, *consumer.consume(num_messages=max(batch_rows - 1, 1), timeout=0)]
        envelopes = []
        for m in messages:
            if m.error():
                logger.warning("consumer error: %s", m.error())
                continue
            if (value := deserialize(m)) is not None:  # tombstones carry no value
                envelopes.append(value)
        if envelopes:
            buffer += scorer.process(envelopes)
        now = clock()
        if messages and not pending:
            since = now
        pending += len(messages)
        if pending and (len(buffer) >= batch_rows or now - since >= batch_seconds):
            commit()
    if pending:
        commit()


class PostgresLookup:
    """Per-batch details query; zip centroids are loaded once (geolocation is reference data)."""

    def __init__(self, conn: Any) -> None:
        self.conn = conn
        rows = conn.execute(CENTROIDS_SQL, BRAZIL_BBOX).fetchall()
        self.centroids = {int(z): (float(lat), float(lng)) for z, lat, lng in rows}
        logger.info("loaded %d zip centroids", len(self.centroids))

    def __call__(self, order_ids: list[str]) -> dict[str, dict[str, Any]]:
        cursor = self.conn.execute(DETAILS_SQL, {"ids": order_ids})
        names = [d[0] for d in cursor.description]
        out = {}
        for values in cursor.fetchall():
            row = dict(zip(names, values, strict=True))
            for who in ("customer", "seller"):
                zip_code = row.pop(f"{who}_zip_code_prefix")
                lat, lng = self.centroids.get(zip_code, (None, None)) if zip_code else (None, None)
                row[f"{who}_lat"], row[f"{who}_lng"] = lat, lng
            out[row.pop("order_id")] = row
        return out


def seller_frame(store: Any) -> Callable[[list[str]], pd.DataFrame]:
    """seller_ids → Feast online `seller_stats` (one read per batch; unseen → NaN)."""

    def read(seller_ids: list[str]) -> pd.DataFrame:
        rows = store.get_online_features(
            features=FEATURE_REFS, entity_rows=[{"seller_id": s} for s in seller_ids]
        ).to_dict()
        return pd.DataFrame({c: rows[c] for c in SELLER_FEATURES}, dtype="float64")

    return read


class LiveWrites:
    def __init__(self, conn: Any, producer: Any, delta_path: str, write_deltalake: Any) -> None:
        self.conn, self.producer = conn, producer
        self.delta_path, self.write_deltalake = delta_path, write_deltalake

    def upsert(self, rows: list[dict[str, Any]]) -> set[str]:
        return upsert_order_risk(self.conn, rows)

    def produce(self, rows: list[dict[str, Any]]) -> None:
        for r in rows:
            self.producer.produce(SCORES_TOPIC, key=r["order_id"], value=kafka_record(r))
        self.producer.poll(0)

    def append(self, frame: pd.DataFrame) -> None:
        self.write_deltalake(self.delta_path, frame, mode="append")

    def flush(self) -> None:
        if left := self.producer.flush(30):
            raise RuntimeError(f"{left} score records not delivered to {SCORES_TOPIC}")


def _champion() -> LoadedModel:
    while (model := load_champion(MODEL_NAME)) is None:
        logger.warning("no %s champion yet; retrying in 30 s", MODEL_NAME)
        time.sleep(30)
    logger.info("scoring with %s v%s", MODEL_NAME, model.version)
    return model


def main() -> int:
    # imported here: only this command needs the Kafka / Postgres / Delta clients
    kafka = importlib.import_module("confluent_kafka")
    registry_mod = importlib.import_module("confluent_kafka.schema_registry")
    avro = importlib.import_module("confluent_kafka.schema_registry.avro")
    serialization = importlib.import_module("confluent_kafka.serialization")
    psycopg = importlib.import_module("psycopg")
    deltalake = importlib.import_module("deltalake")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP") or "127.0.0.1:19092"
    batch_rows = int(os.environ.get("STREAM_BATCH_ROWS") or 500)
    batch_seconds = float(os.environ.get("STREAM_BATCH_SECONDS") or 5)
    metrics = CollectorRegistry()
    start_http_server(METRICS_PORT, registry=metrics)

    if not config.feast_registry_path().is_file():
        raise FeatureStoreUnavailable("Feast registry not found; run `retail-ml materialize`")
    model = _champion()
    conn = psycopg.connect(
        os.environ.get("POSTGRES_DSN") or "postgresql://retail:retail@127.0.0.1:5432/retail",
        autocommit=True,
    )
    producer = kafka.Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True})
    delta_path = (config.gold_dir() / "ml" / "pred_late_delivery_rt").resolve().as_posix()
    scorer = Scorer(
        model=model,
        lookup=PostgresLookup(conn),
        sellers=seller_frame(feature_store()),
        writes=LiveWrites(conn, producer, delta_path, deltalake.write_deltalake),
        registry=metrics,
    )
    schema_registry = registry_mod.SchemaRegistryClient(
        {"url": os.environ.get("SCHEMA_REGISTRY_URL") or "http://127.0.0.1:18081"}
    )
    decode = avro.AvroDeserializer(schema_registry)

    def deserialize(m: Any) -> dict[str, Any] | None:
        if m.value() is None:
            return None
        ctx = serialization.SerializationContext(m.topic(), serialization.MessageField.VALUE)
        value: dict[str, Any] | None = decode(m.value(), ctx)
        return value

    # first start begins at the live end: the topic's history (snapshot + earlier replays) is
    # batch-scored territory and would swamp the latency histogram
    consumer = kafka.Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": GROUP_ID,
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
        }
    )
    consumer.subscribe([SOURCE_TOPIC], on_assign=lambda c, parts: logger.info("assigned %s", parts))
    stop: list[int] = []
    signal.signal(signal.SIGTERM, lambda signum, frame: stop.append(signum))
    logger.info("consuming %s as %s", SOURCE_TOPIC, GROUP_ID)
    try:
        run(consumer, deserialize, scorer, batch_rows, batch_seconds, running=lambda: not stop)
    finally:
        consumer.close()
        conn.close()
    return 0
