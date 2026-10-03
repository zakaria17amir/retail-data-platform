import io
import json
from collections.abc import Sequence
from datetime import datetime
from typing import Any
from urllib.error import URLError

import fastavro
from lakehouse_spark.registry import SchemaNotFound
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

EVENT_V1: dict[str, Any] = {
    "type": "record",
    "name": "ClickstreamEvent",
    "namespace": "retail.events",
    "fields": [
        {"name": "event_id", "type": ["null", "string"], "default": None},
        {"name": "event_type", "type": "string"},
        {"name": "session_id", "type": ["null", "string"], "default": None},
        {"name": "customer_id", "type": ["null", "string"], "default": None},
        {"name": "device", "type": "string"},
        {"name": "referrer", "type": ["null", "string"], "default": None},
        {"name": "event_ts", "type": "string"},
        {"name": "product_id", "type": ["null", "string"], "default": None},
        {"name": "search_query", "type": ["null", "string"], "default": None},
        {"name": "quantity", "type": ["null", "int"], "default": None},
        {"name": "order_id", "type": ["null", "string"], "default": None},
    ],
}
EVENT_V2: dict[str, Any] = {
    **EVENT_V1,
    "fields": [
        *EVENT_V1["fields"],
        {"name": "utm_campaign", "type": ["null", "string"], "default": None},
    ],
}
CDC_ORDERS: dict[str, Any] = {
    "type": "record",
    "name": "Envelope",
    "namespace": "cdc.olist.orders",
    "fields": [
        {
            "name": "before",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "Value",
                    "fields": [{"name": "order_id", "type": "string"}],
                },
            ],
            "default": None,
        },
        {"name": "after", "type": ["null", "Value"], "default": None},
        {"name": "op", "type": "string"},
        {"name": "ts_ms", "type": ["null", "long"], "default": None},
    ],
}

RAW_SCHEMA = StructType(
    [
        StructField("key", BinaryType()),
        StructField("value", BinaryType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
    ]
)


def event(**overrides: Any) -> dict[str, Any]:
    record: dict[str, Any] = {
        "event_id": "e1",
        "event_type": "page_view",
        "session_id": "s1",
        "customer_id": None,
        "device": "mobile",
        "referrer": None,
        "event_ts": "2017-06-01T12:00:00",
        "product_id": None,
        "search_query": None,
        "quantity": None,
        "order_id": None,
    }
    record.update(overrides)
    return record


def encode(schema: dict[str, Any], record: dict[str, Any], schema_id: int) -> bytes:
    buffer = io.BytesIO()
    buffer.write(b"\x00" + schema_id.to_bytes(4, "big"))
    fastavro.schemaless_writer(buffer, fastavro.parse_schema(schema), record)
    return buffer.getvalue()


class FakeRegistry:
    def __init__(self, schemas: dict[int, dict[str, Any]]) -> None:
        self._schemas = schemas

    def get(self, schema_id: int) -> str:
        if schema_id not in self._schemas:
            raise SchemaNotFound(str(schema_id))
        return json.dumps(self._schemas[schema_id])


class BrokenRegistry:
    def get(self, schema_id: int) -> str:
        raise URLError("registry down")


def raw_batch(
    spark: SparkSession,
    rows: Sequence[tuple[bytes | None, str]],
    kafka_ts: datetime = datetime(2017, 6, 1, 12, 0, 0),
) -> DataFrame:
    data = [(None, value, topic, 0, offset, kafka_ts) for offset, (value, topic) in enumerate(rows)]
    return spark.createDataFrame(data, RAW_SCHEMA)
