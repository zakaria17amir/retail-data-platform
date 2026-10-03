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
- **Dedupe:** Kafka key = `event_id`, Kafka timestamp = `event_ts` (dataset time; the sim falls back
  to the order's dataset time for unparseable/out-of-range values so the watermark is not pushed
  forward). `dropDuplicatesWithinWatermark` (48 h) runs on those columns before decoding.
- **Quarantine** (`bronze/_quarantine/{olist,events}`, raw bytes kept): `null_payload`,
  `not_wire_format`, `unknown_schema_id` (registry 404), `avro_decode_failed`, `null_primary_key`,
  `unparseable_timestamp`. An unreachable registry fails the batch so it is retried.
- **Idempotent writes:** Delta `txnAppId` = `{query}[-BRONZE_RUN_ID]-{topic}` / `-quarantine`,
  `txnVersion` = batch id.

## Consequences
- Duplicates arriving more than 48 h (event time) apart reach bronze; silver dedupes them.
- Resetting checkpoints requires clearing `bronze/` or a new `BRONZE_RUN_ID`, else Delta skips the
  replayed batch ids as already committed.

| Component | Version |
|---|---|
| Redpanda / Console | v26.2.3 / v3.12.0 |
| cp-kafka-connect-base (Kafka 4.2) + Debezium Postgres | 8.2.4 + 3.7.0.Final |
| apache/spark (scala2.13, java17, python3) / pyspark / delta-spark | 4.0.4 / 4.0.4 / 4.0.1 |
| hadoop-aws + AWS SDK v2 bundle / kafka-clients | 3.4.1 + 2.24.6 / 3.9.1 |
| confluent-kafka (simulator) | 2.15.1 |
