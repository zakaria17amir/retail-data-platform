# Lakehouse runbook (Phase 2, silver)

Bronze Delta → Spark batch → silver Delta on MinIO (`s3a://lakehouse/silver/<domain>/<table>`), then
Great Expectations gates. Design and rule ids: [ADR-0004](../adr/0004-silver-design.md). Bronze must
exist first ([ingestion runbook](ingestion.md)). Run from the repo root in Git Bash.

## Run

```sh
make silver                                               # all tables
make silver SILVER_ARGS="--tables sales/orders,sales/order_items"
make quality                                              # exit 1 on any critical failure
make maintain                                             # OPTIMIZE … ZORDER BY + VACUUM (168 h)
make status                                               # bronze + silver counts, rejects, rule metrics
```

`make silver` runs the job in a one-off `spark` container. Each bronze table is read incrementally
(`availableNow`, checkpoint `_checkpoints/silver/<domain>_<table>`; events: one per type,
`events_clickstream_<type>` for `add_to_cart`, `checkout_started`, `page_view`, `product_view`,
`search`), so a rerun
with no new bronze data changes nothing. Table order matters for `catalog/products` (needs
`catalog/categories`) and `geo/zip_centroids` (rebuilt from `geo/geolocation_points`). With no bronze
events source yet, the job creates an empty `events/clickstream`, so `make quality` warns
`freshness observed=empty` until the first events arrive.

Memory: the silver container is limited to 4 GiB and peaks at ~3 GiB on full data (`maintain` ~3.4 GiB);
next to the `ingest` profile the container limits add up to ~13.3 GiB. Run `make silver` before
`make maintain` after a pause of more than 7 days: VACUUM (168 h) can remove bronze files a stalled
silver stream still points at.

`make quality` runs on the host (`deltalake` + pandas, no Spark), prints one
`PASS|FAIL <severity> <table> <expectation> observed=…` line per check and appends them to
`silver/_dq_results`. Critical: `table_exists`, `table_readable`, columns, key not null/unique,
row count vs bronze minus rejects (`DQ_ROWCOUNT_TOLERANCE`, default 0.001), referential integrity,
`price > 0`. Warnings: review score, freight p99, events `product_id` in products, freshness
(`DQ_FRESHNESS_HOURS`, default 2).

## Reading rule metrics

`make status` lists each `silver/*` table (SCD2: current rows), each `silver/_rejects/*` table, and,
for the latest run in `silver/_rule_metrics`, one line per rule that rejected rows:

```
rule geo_out_of_bbox rejected 27 rows (0.003 %) in geo/geolocation_points
```

## Inspecting rejects

Rejects keep `rule_id`, `reason`, the row as `record_json`, `_run_id`, `_rejected_at`:

```sh
uv run --package lakehouse-quality python -c "
from deltalake import DeltaTable
from lakehouse_quality.io import storage_options
options = storage_options('s3://lakehouse')
t = DeltaTable('s3://lakehouse/silver/_rejects/geo/geolocation_points', storage_options=options)
print(t.to_pandas(columns=['rule_id', 'reason', 'record_json']).head(20))
"
```

`storage_options` reads `MINIO_ENDPOINT` and `MINIO_ROOT_*` from the environment (defaults match
`.env.example`). Same pattern for `silver/_rule_metrics` (`run_id`, `table`, `rule_id`, `rows_in`, `rows_rejected`,
`pct_rejected`) and `silver/_dq_results`.

## Reset silver

Delete the silver tables and their checkpoints together (a checkpoint without its table skips data):

```sh
docker compose --profile ingest exec -T minio sh -c \
  'mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null \
   && mc rm -r --force local/lakehouse/silver local/lakehouse/_checkpoints/silver'
make silver
```

Bronze and its checkpoints are untouched; the next `make silver` rebuilds silver from all of bronze.

Deleting only `_checkpoints/silver` (keeping the tables) also reprocesses all of bronze: silver
converges, but rejects and rule metrics are appended again, and every record already in silver is
re-rejected (`cdc_exact_duplicate` "… (already in silver)", `event_duplicate` "event_id already in
silver"). Clear
both unless that duplicate reject history is what you want.
