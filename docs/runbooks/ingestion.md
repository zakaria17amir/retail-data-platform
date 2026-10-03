# Ingestion runbook (Phase 1)

Postgres → Debezium (Kafka Connect) → Redpanda `cdc.olist.*`, clickstream-sim → Redpanda
`events.*`, Spark Structured Streaming → bronze Delta on MinIO. Serialization, dedupe and
quarantine rules are in [ADR-0003](../adr/0003-ingestion-serialization.md). Run every command from
the repo root in Git Bash; prefix raw `docker` commands that take `/paths` with `MSYS_NO_PATHCONV=1`.

## Start

```sh
make up                 # core: postgres, minio
make seed               # or: make seed-sample (200 orders, no Kaggle account needed)
make up PROFILE=ingest  # + redpanda, console, kafka-connect, connector registration, spark
make status
```

The connector snapshots whatever is in Postgres when it is first registered (full Olist: ~32 s),
so load the database first, or run `make reset-bronze` afterwards. Spark then drains the snapshot
(1.41M rows, ~14 min at 50k offsets per micro-batch, ~25 s per batch).

`make up` does not rebuild images. After changing `ingestion/debezium/` or `lakehouse/spark/`:

```sh
docker compose --profile ingest build spark
docker compose --profile ingest up -d --force-recreate spark   # `restart` keeps the old image
```

## Replay and clickstream

```sh
make replay REPLAY_ARGS="--from 2017-10-02 --until 2017-10-03 --speed 7200"
make sim SIM_ARGS="--max-events 2000"
```

One dataset day at speed 7200 takes ~10 min: the lifecycle updates of orders placed that day span
~75 dataset days after the window. Long inactivity
gaps sleep `gap / speed`. The simulator follows new orders from CDC and is tuned by the `SIM_*`
variables in `.env` (late, duplicate and corrupt rates, schema evolution after N events). As
long-running containers instead: `docker compose --profile demo up -d replayer clickstream-sim`.

## Watch it

- **Redpanda Console**, http://127.0.0.1:${CONSOLE_PORT:-8080}: *Topics* (`cdc.olist.*`,
  `events.*` message counts; messages decode via the registry), *Schema Registry* (a `<topic>-value`
  subject per topic; a second version of an `events.*-value` subject after schema evolution), *Connect*
  (`olist-postgres` RUNNING with one task). An UPDATE reaches its topic in ~0.5 s.
- **Spark UI**, http://127.0.0.1:4040: *Structured Streaming* shows `cdc_to_bronze` and
  `events_to_bronze` with input/processing rates and batch durations.

## Reading `make status`

The first block (replayer) is Postgres row counts. The second (`bronze status`) is Delta row
counts per table: `bronze/olist/<table>`, `bronze/events/<type>`, `quarantine/olist`,
`quarantine/events`, and `quarantine/<source> reasons` (count per reason). After the snapshot each
`bronze/olist/<table>` equals the Postgres count (plus one row per later change) and
`quarantine/olist` is 0. Events: bronze + quarantine ≈ events sent minus deduplicated duplicates
(smoke run: 2000 events → 1907 bronze + 93 quarantine, 12 duplicates dropped).

## Quarantine

Reasons: `null_payload` (tombstone/empty value), `not_wire_format` (no `0x00` + schema id
header), `unknown_schema_id` (registry 404), `avro_decode_failed`, `null_primary_key`,
`unparseable_timestamp`. A registry outage fails the batch (Spark retries) instead of
quarantining. Rows keep the Kafka coordinates and raw bytes:

```sh
uv run --package lakehouse-spark python -c "
import os
from deltalake import DeltaTable
from lakehouse_spark.cli import storage_options
t = DeltaTable('s3://lakehouse/bronze/_quarantine/events', storage_options=storage_options('s3://lakehouse', os.environ))
print(t.to_pyarrow_table(columns=['reason', 'kafka_topic', 'kafka_offset', 'raw_value']).slice(0, 20))
"
```

`storage_options` targets MinIO at `127.0.0.1:${MINIO_PORT}` with the `MINIO_ROOT_*` credentials
(defaults from `.env.example`). Use `_quarantine/olist` for CDC rows.

## Reset bronze

```sh
make reset-bronze SEED=seed   # or SEED=seed-sample
```

`ingestion/reset-bronze.sh` stops spark; stops the `olist-postgres` connector and deletes its
offsets; drops the `olist_debezium` replication slot; deletes the `cdc.olist.*` and `events.*`
topics and the `clickstream-sim` consumer group; `mc rm`s `bronze/` and `_checkpoints/` in the
bucket; reloads Postgres; re-registers the connector (`connect-init`), waits up to 900 s for
"Snapshot completed"; starts spark.

Never delete `_checkpoints/bronze/*` alone: Delta writes use `txnAppId` + `txnVersion` = batch id,
so restarted batch ids 0, 1, … would be skipped as already committed. To restart from empty
checkpoints while keeping the tables, set a new `BRONZE_RUN_ID` in `.env`, clear
`_checkpoints/bronze`, then `docker compose --profile ingest up -d --force-recreate spark`.

## Memory

Ingest-profile limits: spark 4 GiB (1.3-1.7 GiB used while draining the full snapshot), redpanda
and kafka-connect 1.5 GiB each, postgres and minio 1 GiB, console 256 MiB: ~9.3 GiB, which fits
the 16 GB `.wslconfig`. The Redpanda volume holds ~2 GB after the full snapshot.

## `make test-ingest`

**Destructive.** It runs `reset-bronze.sh seed-sample`: Postgres is reseeded with the 200-order
sample, topics and bronze are wiped. It refuses to run without `ALLOW_RESEED=1`:

```sh
ALLOW_RESEED=1 make test-ingest
make reset-bronze SEED=seed   # restore the full dataset afterwards
```
