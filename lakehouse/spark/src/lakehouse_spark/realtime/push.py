"""Feast feature server `/push` client (stdlib only: the spark image has no requests)."""

import json
import logging
import time
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

CHUNK_ROWS = 500
RETRYABLE_4XX = (408, 429)
SKIPPED_BATCHES: Counter[str] = Counter()
log = logging.getLogger("realtime.push")


TIMESTAMP_FIELD = "event_ts"


def _value(column: str, v: Any) -> Any:
    if not isinstance(v, datetime):
        return v
    # Feast parses the event timestamp with pd.to_datetime but int()-casts UnixTimestamp features
    if column == TIMESTAMP_FIELD:
        return v.isoformat()
    return int(v.replace(tzinfo=v.tzinfo or timezone.utc).timestamp())  # noqa: UP017 (spark: py3.10)


def _payload(source: str, rows: Sequence[Mapping[str, Any]]) -> bytes:
    columns = {c: [_value(c, r[c]) for r in rows] for c in rows[0]}
    return json.dumps({"push_source_name": source, "df": columns, "to": "online"}).encode()


def _bad_payload(e: HTTPError) -> bool:
    return 400 <= e.code < 500 and e.code not in RETRYABLE_4XX


def _post(url: str, body: bytes, attempts: int, backoff_s: float, timeout_s: float) -> None:
    request = Request(url, body, {"Content-Type": "application/json"}, method="POST")
    for attempt in range(1, attempts + 1):
        try:
            with urlopen(request, timeout=timeout_s) as response:
                response.read()
            return
        except HTTPError as e:
            if _bad_payload(e) or attempt == attempts:
                raise
        except OSError:
            if attempt == attempts:
                raise
        time.sleep(backoff_s * attempt)


def push_rows(
    base_url: str,
    source: str,
    rows: Sequence[Mapping[str, Any]],
    *,
    chunk_rows: int = CHUNK_ROWS,
    attempts: int = 3,
    backoff_s: float = 1.0,
    timeout_s: float = 10.0,
) -> bool:
    """Push rows in chunks; False when Feast rejects the payload (4xx) and the batch is skipped.

    Server errors and connection failures raise so the micro-batch fails and Spark retries it.
    """
    url = f"{base_url.rstrip('/')}/push"
    for start in range(0, len(rows), chunk_rows):
        body = _payload(source, rows[start : start + chunk_rows])
        try:
            _post(url, body, attempts, backoff_s, timeout_s)
        except HTTPError as e:
            if not _bad_payload(e):
                raise
            SKIPPED_BATCHES[source] += 1
            log.error(
                "%s rejected the payload (HTTP %d: %s); skipped the batch (%d skipped so far)",
                source,
                e.code,
                e.read()[:500].decode(errors="replace"),
                SKIPPED_BATCHES[source],
            )
            return False
    return True


def latest_per_key(df: DataFrame, key: str) -> DataFrame:
    newest = Window.partitionBy(key).orderBy(F.col("event_ts").desc())
    return df.withColumn("_rn", F.row_number().over(newest)).filter("_rn = 1").drop("_rn")


def push_batch(batch: DataFrame, batch_id: int, *, base_url: str, source: str, key: str) -> None:
    rows = [r.asDict() for r in latest_per_key(batch, key).collect()]
    if push_rows(base_url, source, rows):
        log.info("%s batch %d pushed %d rows", source, batch_id, len(rows))
