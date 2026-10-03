# ADR-0003: Avro + Schema Registry for ingestion, decoded per schema id in Spark

## Status
Accepted

## Context
Debezium CDC and the clickstream simulator both write to Redpanda; Spark must land them in bronze
Delta without losing rows to schema evolution, late or duplicated events, or bad payloads.

## Decision
- **Wire format:** Avro with the Confluent framing (`0x00` + 4-byte schema id) against Redpanda's
  Confluent-compatible registry. Connect runs on `cp-kafka-connect-base` + the Debezium Postgres
  plugin: the `debezium/connect` image ships only Apicurio converters (checked in its Dockerfile).
  `time.precision.mode=connect`, `decimal.handling.mode=precise`.
- **Decoding:** `foreachBatch` groups each topic's rows by schema id and runs `from_avro` with that
  exact writer schema, so a v2 schema adds columns instead of misdecoding. PERMISSIVE `from_avro`
  returns an all-null struct on failure, so a row is "undecoded" when its first non-nullable field
  is null.
- **Dedupe:** Kafka key = `event_id` (identity-derived, `uuid5` of `session_id:step`, so reruns
  never collide), Kafka timestamp = `event_ts` (dataset time; the sim falls back to the order's
  dataset time for bad values, never pushing the watermark ahead). `dropDuplicatesWithinWatermark`
  runs before decoding; watermark 72 h (sim lateness ≤ 48 h + session/browsing offsets + margin).
  Rows behind it, late originals and duplicates alike, are dropped and logged per batch as a
  WARNING with `numRowsDroppedByWatermark`; they are not quarantined.
- **Quarantine** (`bronze/_quarantine/{olist,events}`, raw bytes kept): `null_payload`,
  `not_wire_format`, `unknown_schema_id` (registry 404), `avro_decode_failed`, `null_primary_key`,
  `unparseable_timestamp` (incl. `event_ts` > `kafka_timestamp` + 1 day: dataset time, not wall
  clock). An unreachable registry fails the batch so it is retried.
- **Idempotent writes:** Delta `txnAppId` = `{query}[-BRONZE_RUN_ID]-{topic}` / `-quarantine`,
  `txnVersion` = batch id.

## Consequences
- Duplicates > 72 h (event time) late are dropped behind the watermark, not written to bronze.
- Forward-only: the watermark persists in the checkpoint, so replay windows must move forward in
  dataset time relative to what bronze has seen; otherwise reset with `make reset-bronze`.
- New checkpoints need a cleared `bronze/` or a new `BRONZE_RUN_ID`, else Delta skips old batch ids.
- Host deps: `deltalake`, `pyarrow` (bronze status), `fastavro` (tests), host `pyspark` (workspace).

| Component | Version |
|---|---|
| Redpanda / Console | v26.2.3 / v3.12.0 |
| cp-kafka-connect-base (Kafka 4.2) + Debezium Postgres | 8.2.4 + 3.7.0.Final |
| apache/spark (scala2.13, java17, python3) / pyspark / delta-spark | 4.0.4 / 4.0.4 / 4.0.1 |
| hadoop-aws + AWS SDK v2 bundle / kafka-clients | 3.4.1 + 2.24.6 / 3.9.1 |
| confluent-kafka (simulator) | 2.15.1 |
