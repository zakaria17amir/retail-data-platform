# ADR-0004: Silver as incremental Delta streams with latest-LSN MERGE and CDC-driven SCD2

## Status
Accepted

## Context
Bronze is append-only Delta (one row per CDC record or event). Silver must be cleaned, typed, idempotent
on reruns, keep history for products/customers/sellers, and account for every dropped row.

## Decision
- **Incremental read:** one Delta streaming read per bronze table, `trigger(availableNow=True)`,
  checkpoint `_checkpoints/silver/<domain>_<table>` (events: one per type,
  `events_clickstream_<type>`). Not Change Data Feed: bronze never updates rows, so CDF adds nothing;
  it stays reserved for tables with updates.
- **Idempotency:** writes carry Delta `txnAppId = silver-<checkpoint>-<query id>`,
  `txnVersion = batch id`; a replayed batch is skipped, a new checkpoint is a new namespace;
  `maxBytesPerTrigger = 256m`; schema-change restart ×3.
- **Current-state tables:** collapse each batch to the latest `(_source_lsn, kafka_offset)` per key, then
  `MERGE … WHEN MATCHED AND s._source_lsn >= t._source_lsn`, so out-of-order batches never regress.
  Deletes stay as rows with `_is_deleted = true` (values from `before`, which is the full row because
  the Olist tables are `REPLICA IDENTITY FULL`; otherwise Debezium sends only the PK and domain rules
  would reject the delete).
- **SCD2** (processing time from CDC): one MERGE keyed `(key, _source_lsn)` inserts new versions and
  rewrites `valid_to`/`is_current` for all rows of touched keys; a rerun is a no-op. A change whose
  hash of tracked columns + `_is_deleted` equals its predecessor's (only `updated_at` changed) makes no
  version. Snapshot (`op = r`) first versions start `1900-01-01`; a delete is a version.
- **Olist item "duplicates"** are quantity units (distinct `order_item_id`), kept. True duplicates are
  redelivered CDC records with the same key and LSN, in the batch or already in silver.
- **Quality gates:** Great Expectations 1.23.2 on pandas read with `deltalake`: no Spark in the gates.
- **Layout:** only `events/clickstream` is partitioned (`event_date`); orders (~100k rows) would give
  tiny partitions. Z-order: `sales/orders` (`customer_id`), `sales/order_items` (`product_id`,
  `seller_id`), `events/clickstream` (`session_id`). `VACUUM` keeps 168 h.
- **Rule ids** (rejects land in `silver/_rejects/<domain>/<table>`; metrics in `silver/_rule_metrics`):
  rejecting `cdc_exact_duplicate`, `orders_null_purchase_ts`, `geo_out_of_bbox`, `event_cast_failed`,
  `event_negative_quantity`, `event_duplicate`, `session_over_24h`; non-rejecting (`rows_rejected = 0`)
  `cdc_collapse`, `ts_localise`, `orders_timeline_flags`, `category_translate`, `rename_columns`.

## Consequences
- Superseded versions of current-state tables are not in silver (history stays in bronze).
- SCD2 `valid_from` is CDC commit time, not business time; pre-snapshot history is unknown.
- GE needs the whole table in memory; fine at Olist scale, revisit for larger tables.
- `REPLICA IDENTITY FULL` makes WAL and update `before` structs larger.
- A checkpoint-only reset re-rejects every CDC record and event already in silver.
