# Phase 1 — Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Olist changes (replayed in time-compressed order) and a grounded synthetic clickstream flow
through Redpanda into append-only bronze Delta tables on MinIO, with every undecodable or insane
record landing in a quarantine table with a reason, and an integration test proving
`bronze + quarantine == source`.

**Architecture:** `replayer live` upserts Olist rows into Postgres on a compressed clock; Debezium
(Kafka Connect) publishes them as Avro to `cdc.olist.<table>`. `clickstream-sim` consumes the
order/order_items CDC topics and produces Avro browsing sessions to `events.<type>`, with late,
duplicate, malformed and schema-evolved records injected on purpose. One PySpark Structured
Streaming app runs two queries (`cdc_to_bronze`, `events_to_bronze`); each decodes registry-framed
Avro per schema id inside `foreachBatch`, writes good rows to `bronze/...` and bad rows to
`bronze/_quarantine/...`. Events are deduplicated on the Kafka key (= `event_id`) under a 48 h
watermark on the Kafka timestamp (= `event_ts`) before decoding.

**Tech Stack:** Redpanda (+ Schema Registry, Console), Confluent `cp-kafka-connect-base` +
Debezium Postgres connector, PySpark Structured Streaming + Delta Lake + `spark-avro` + `hadoop-aws`,
`confluent-kafka[avro]` (Python producer/consumer), `deltalake` (host-side Delta reads), polars,
psycopg, uv, Docker Compose profile `ingest`.

**Spec:** `docs/superpowers/specs/retail-data-platform-design.md` §4.2, §4.3, §5, §12, §16.
Phase 0 is merged on `main` (`8b3b226`); this plan builds on it.

## Global Constraints

- Python 3.12 via the project-local uv venv; `uv run --package <pkg> …`; one root `uv.lock`.
- `ruff check`, `ruff format --check`, `mypy --strict` clean on every package (`make lint` must cover
  the new packages).
- Docker images pinned `tag@sha256:…`; every service has a memory limit; host ports bind to
  `127.0.0.1` only. Containers reach Postgres as `postgres:5432`, Redpanda as `redpanda:9092`,
  Schema Registry as `http://redpanda:8081`, MinIO as `http://minio:9000`.
- Config via env vars with defaults documented in `.env.example`; no secrets committed.
- Secrets/ports/images changes belong to `platform-engineer`; `ingestion/`, `lakehouse/` and
  `tests/` code to `data-engineer`. Exceptions are called out per task.
- Conventional commits; each task commits on the shared branch `phase-1-ingestion`.
- Nothing is dropped silently: every record that does not reach bronze reaches quarantine with a
  `reason`, or the batch fails loudly.
- All event-time values stay in **dataset time** (2016-2018); the compressed clock only governs
  wall-clock emission. Replay speed is a single shared setting `REPLAY_SPEED` (dataset seconds per
  wall second).

## Review Focus

Inputs the spec implies but no task's happy path exercises; each has its test pinned in the owning
task.

1. A Kafka record with a **null value** (Debezium tombstone, producer bug) must land in quarantine
   with reason `null_payload`, never crash a batch. → Task 5.
2. A **non-registry-framed payload** (first byte ≠ 0, or shorter than 5 bytes, e.g. someone
   `rpk produce`s plain text onto a topic) → quarantine `not_wire_format`. → Task 5.
3. **Schema Registry unreachable** (connection refused) must fail the micro-batch so Spark retries,
   whereas a **404 for an unknown schema id** quarantines the record (`unknown_schema_id`).
   Transient and permanent failures must not be confused. → Task 5.
4. **`replayer live` restarted mid-run** (or run twice over the same window) must not raise primary
   key violations and must leave the same final table state; it emits CDC `u` events for the
   repeats, which is acceptable. → Task 3.
5. Olist orders with **missing intermediate timestamps** (e.g. `order_approved_at` null while
   delivered dates exist, or `delivered_customer_date` before `delivered_carrier_date`) must
   produce a timeline whose steps are in non-decreasing `ts` order per order and never raise.
   → Task 3.

Known and documented, not tested: a duplicate arriving more than 48 h (dataset time) after its
original is not deduplicated in bronze; silver's `event_id` dedupe rule catches it (Phase 2).

## File Structure

```
ingestion/
  replayer/src/replayer/timeline.py      Change dataclass, build_timeline, shift_history  (T3)
  replayer/src/replayer/live.py          apply_change, run_live                            (T3)
  replayer/src/replayer/cli.py           + `live` subcommand                               (T3)
  replayer/Dockerfile                                                                      (T3)
  clickstream_sim/pyproject.toml         package `clickstream-sim`, module clickstream_sim (T4)
  clickstream_sim/src/clickstream_sim/events.py    Event, Catalogue, OrderRef, session generators (T4)
  clickstream_sim/src/clickstream_sim/faults.py    Emission, FaultConfig, inject               (T4)
  clickstream_sim/src/clickstream_sim/producer.py  Avro producer + schema registration         (T6)
  clickstream_sim/src/clickstream_sim/cdc.py       CDC consumer of orders/order_items          (T6)
  clickstream_sim/src/clickstream_sim/cli.py       `clickstream-sim run`                       (T6)
  clickstream_sim/Dockerfile                                                                   (T6)
  schemas/events/clickstream_event.v1.avsc, .v2.avsc                                           (T4)
  debezium/Dockerfile                    cp-kafka-connect-base + Debezium Postgres plugin     (T1)
  debezium/olist-postgres.json           connector config                                     (T2)
  debezium/register.sh                   idempotent PUT of the config, used by connect-init   (T2)
lakehouse/
  spark/pyproject.toml                   package `lakehouse-spark`, module lakehouse_spark   (T5)
  spark/Dockerfile                       apache/spark + delta/kafka/avro/s3a jars + pytest    (T1)
  spark/src/lakehouse_spark/wire.py      parse_wire_header (pure)                            (T5)
  spark/src/lakehouse_spark/registry.py  SchemaRegistry (urllib, cached)                     (T5)
  spark/src/lakehouse_spark/bronze/decode.py   decode_batch                                  (T5)
  spark/src/lakehouse_spark/bronze/sink.py     write_bronze, write_quarantine                (T5)
  spark/src/lakehouse_spark/bronze/app.py      session, two queries, main                    (T5)
  spark/src/lakehouse_spark/cli.py       `bronze status` via deltalake                       (T7)
  spark/tests/                           host tests + `spark`-marked container tests         (T5)
tests/integration/test_ingest.py         end-to-end `ingest`-marked test                     (T7)
docker-compose.yml                       profile `ingest`                                    (T1, T2)
Makefile                                 up/test-spark/replay/sim/test-ingest/status         (T1, T7)
.github/workflows/ci.yml                 job `ingest`                                        (T8)
docs/adr/0003-ingestion-serialization.md, docs/runbooks/ingestion.md, README.md             (T8)
```

Waves: **1** = T1 ∥ T3 ∥ T4 → **2** = T2 ∥ T5 ∥ T6 (T6 needs T2's topics to exist) → **3** = T7 → T8.

---

### Task 1: Compose `ingest` profile — Redpanda, Console, Kafka Connect image, Spark image

**Owner:** `platform-engineer`. Exception granted: may create `ingestion/debezium/Dockerfile` and
`lakehouse/spark/Dockerfile` (image builds are platform concerns).

**Files:**
- Modify: `docker-compose.yml`, `Makefile`, `.env.example`, `.gitignore` (add `spark-warehouse/`,
  `metastore_db/`, `derby.log`)
- Create: `ingestion/debezium/Dockerfile`, `lakehouse/spark/Dockerfile`

**Interfaces:**
- Produces, for later tasks:
  - Services (profile `ingest`, all also start `core`'s postgres/minio via `depends_on`):
    `redpanda` (internal `redpanda:9092`, registry `redpanda:8081`; host `127.0.0.1:${REDPANDA_PORT:-19092}`
    and `127.0.0.1:${SCHEMA_REGISTRY_PORT:-18081}`), `redpanda-console` (`127.0.0.1:${CONSOLE_PORT:-8080}`),
    `kafka-connect` (REST `kafka-connect:8083`, host `127.0.0.1:${CONNECT_PORT:-8083}`), `spark`
    (UI `127.0.0.1:4040`; runs `spark-submit /opt/lakehouse/src/lakehouse_spark/bronze/app.py`;
    mounts `./lakehouse/spark:/opt/lakehouse:ro`), `replayer` and `clickstream-sim` services are
    **added in T3/T6**, not here.
  - Env vars in `.env.example` (host-side defaults): `REDPANDA_PORT=19092`,
    `SCHEMA_REGISTRY_PORT=18081`, `CONSOLE_PORT=8080`, `CONNECT_PORT=8083`,
    `KAFKA_BOOTSTRAP=127.0.0.1:19092`, `SCHEMA_REGISTRY_URL=http://127.0.0.1:18081`,
    `KAFKA_CONNECT_URL=http://127.0.0.1:8083`, `REPLAY_SPEED=3600`.
    Containers override `KAFKA_BOOTSTRAP=redpanda:9092`, `SCHEMA_REGISTRY_URL=http://redpanda:8081`
    in their `environment:` blocks.
  - Spark image contract: `pyspark` + `delta-spark` Python packages and `pytest` installed; jars for
    Delta, `spark-sql-kafka-0-10`, `spark-avro`, `hadoop-aws` + AWS SDK bundle baked into
    `$SPARK_HOME/jars` (no `--packages` at runtime). Spark config set via `spark-defaults.conf` in
    the image: Delta extensions/catalog, S3A endpoint `http://minio:9000`, path-style access,
    `fs.s3a.access.key`/`secret.key` from `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD` env (use
    `spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider`
    and set the keys via `SPARK_SUBMIT_OPTS`/env-substituted conf, not hard-coded),
    `spark.sql.shuffle.partitions=4`, `spark.driver.memory=2g`, master `local[2]`.
  - Make targets: `make up PROFILE=ingest` (reuses the existing init check), `make test-spark`
    (`docker compose --profile ingest run --rm --no-deps spark pytest /opt/lakehouse/tests -m spark`).

- [ ] **Step 1: Pick and pin versions.** Choose the newest stable releases as of today and record
  them in a comment at the top of each Dockerfile: Redpanda `redpandadata/redpanda`, Console
  `redpandadata/console`, Confluent `cp-kafka-connect-base` (Kafka 3.9+), Debezium PostgreSQL
  connector (3.x, the `-plugin.tar.gz` from
  `https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/<v>/`), `apache/spark`
  `-python3` image and the Delta Lake release listed as compatible with it on the Delta docs
  compatibility matrix (Spark 3.5.x↔Delta 3.3.x or Spark 4.0.x↔Delta 4.0.x; pick one pair, Scala
  version of the jars must match the image). Pin all images `tag@sha256:…`
  (`docker buildx imagetools inspect <image:tag>` gives the digest).

- [ ] **Step 2: `ingestion/debezium/Dockerfile`.** `FROM confluentinc/cp-kafka-connect-base:<tag>@sha256:…`;
  download the Debezium plugin tarball with `curl -fsSL … | tar xz -C /usr/share/confluent-hub-components`;
  verify the Avro converter exists (`ls /usr/share/java/kafka-serde-tools | grep -i avro` in a
  RUN step so the build fails if the base image stops shipping it).

- [ ] **Step 3: `lakehouse/spark/Dockerfile`.** `FROM apache/spark:<tag>@sha256:…`; `curl` the jars
  listed in Interfaces from Maven Central into `$SPARK_HOME/jars` (exact coordinates for the chosen
  versions; `hadoop-aws` must match the Hadoop version bundled in the image — check
  `ls $SPARK_HOME/jars | grep hadoop-client`); `pip install --no-cache-dir pyspark==<same> delta-spark==<same> pytest fastavro`;
  copy `spark-defaults.conf`; `USER spark`.

- [ ] **Step 4: Compose services.** Add `redpanda` (`redpanda start --mode dev-container --smp 1 --memory 1G --overprovisioned --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092 --advertise-kafka-addr internal://redpanda:9092,external://127.0.0.1:${REDPANDA_PORT} --schema-registry-addr internal://0.0.0.0:8081,external://0.0.0.0:18081`;
  healthcheck `rpk cluster health`; memory limit `1500m`; volume `redpandadata`), `redpanda-console`
  (config env/yaml so Console shows topics, schema registry and the Connect cluster — verify key
  names for the chosen Console version; limit `256m`), `kafka-connect` (`build: ingestion/debezium`;
  env `CONNECT_BOOTSTRAP_SERVERS=redpanda:9092`, `CONNECT_GROUP_ID=retail-connect`, the three
  `CONNECT_*_STORAGE_TOPIC`s with replication factor 1, `CONNECT_KEY_CONVERTER`/`CONNECT_VALUE_CONVERTER=io.confluent.connect.avro.AvroConverter`,
  `CONNECT_*_CONVERTER_SCHEMA_REGISTRY_URL=http://redpanda:8081`, `CONNECT_PLUGIN_PATH=/usr/share/java,/usr/share/confluent-hub-components`,
  `CONNECT_REST_ADVERTISED_HOST_NAME=kafka-connect`, `KAFKA_HEAP_OPTS=-Xms256m -Xmx1g`; healthcheck
  `curl -fs http://localhost:8083/connectors`; limit `1500m`), `spark` (`build: lakehouse/spark`;
  env `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `LAKEHOUSE_BUCKET`, `KAFKA_BOOTSTRAP=redpanda:9092`,
  `SCHEMA_REGISTRY_URL=http://redpanda:8081`; command `spark-submit /opt/lakehouse/src/lakehouse_spark/bronze/app.py`;
  `depends_on` redpanda+minio healthy; limit `3g`; `restart: unless-stopped`). All on network
  `retail`, profile `ingest`.

- [ ] **Step 5: Makefile + `.env.example` + `.gitignore`** per Interfaces.

- [ ] **Step 6: Verify.** `docker compose config --quiet`; `make up PROFILE=ingest` exits 0 (spark will
  crash-loop until T5 provides `app.py` — that is expected; note it in the report);
  `curl -fs http://127.0.0.1:8083/connector-plugins | grep -c PostgresConnector` prints `1`;
  `curl -fs http://127.0.0.1:18081/subjects` prints `[]`; Console loads at `http://127.0.0.1:8080`
  and shows the Connect cluster; `docker compose --profile ingest run --rm --no-deps spark python -c "import pyspark, delta; print(pyspark.__version__)"`
  prints the pinned version; `docker stats --no-stream` total for the ingest containers < 7 GB.
  `make lint` still passes.

- [ ] **Step 7: Commit** `feat(platform): compose ingest profile with redpanda, connect and spark images`.

---

### Task 2: Debezium connector for `olist` → `cdc.olist.<table>` (Avro)

**Owner:** `data-engineer`. Depends on T1. Exception granted: add the `connect-init` service to
`docker-compose.yml` (copy the `minio-init` pattern exactly; nothing else in that file).

**Files:**
- Create: `ingestion/debezium/olist-postgres.json`, `ingestion/debezium/register.sh`
- Modify: `docker-compose.yml` (service `connect-init` only), `Makefile` (`up` init check must
  also require `connect-init exited 0`).

**Interfaces:**
- Produces: topics `cdc.olist.<table>` for the nine Olist tables, value = Debezium envelope in Avro
  (subjects `cdc.olist.<table>-value` in the registry), key = Avro of the primary key. Envelope
  fields relied on downstream: `op` (`r|c|u|d`), `ts_ms`, `before`, `after`, `source.lsn`,
  `source.ts_ms`, `source.table`. Timestamps are Avro `timestamp-millis`
  (`time.precision.mode=connect`), decimals Avro `decimal` (`decimal.handling.mode=precise`).
- Connector name `olist-postgres`; replication slot `olist_debezium`; publication `olist_cdc`
  (exists from Phase 0; `publication.autocreate.mode=disabled`); `topic.prefix=cdc`;
  `schema.include.list=olist`; `snapshot.mode=initial`; `tombstones.on.delete=false`;
  `heartbeat.interval.ms=10000`; `plugin.name=pgoutput`; `database.hostname=postgres`, credentials
  from `${POSTGRES_USER}`/`${POSTGRES_PASSWORD}`/`${POSTGRES_DB}` substituted by `register.sh`
  (`envsubst` or `sed`; the JSON file in git contains `${POSTGRES_PASSWORD}` placeholders, never a
  value). Converters at connector level: `io.confluent.connect.avro.AvroConverter` with
  `schema.registry.url=http://redpanda:8081` for key and value.
- `register.sh`: waits for `GET /connectors` (max 120 s), then `PUT /connectors/olist-postgres/config`
  with the substituted JSON (PUT is create-or-update, hence idempotent), then polls
  `GET /connectors/olist-postgres/status` until `connector.state == RUNNING` and every task is
  `RUNNING` (max 60 s) or exits 1 printing the status body.

- [ ] **Step 1: Write the config and script.** `connect-init` service: image `curlimages/curl:<tag>@sha256:…`
  (platform convention: pinned), `depends_on: kafka-connect: condition: service_healthy`, mounts
  `./ingestion/debezium:/debezium:ro`, env from `.env`, `entrypoint: ["sh", "/debezium/register.sh"]`,
  `restart: "no"`, limit `128m`. Note: `curlimages/curl` lacks `envsubst`; use `sed` substitutions.

- [ ] **Step 2: Verify on real data.** With `core` seeded with the full Olist dataset
  (`make seed` if `make status` does not show `orders: 99441`): `make up PROFILE=ingest`
  (expect `connect-init exited 0`); `curl -fs http://127.0.0.1:18081/subjects` lists nine
  `cdc.olist.*-value` subjects; `rpk -X brokers=127.0.0.1:19092 topic list` shows nine `cdc.olist.*`
  topics; `rpk … topic consume cdc.olist.orders -n 1 -f '%v\n'` shows an Avro-framed value
  (unreadable bytes starting `\x00\x00\x00\x00`). Then
  `psql "$POSTGRES_DSN" -c "UPDATE olist.orders SET order_status=order_status WHERE order_id=(SELECT order_id FROM olist.orders LIMIT 1)"`
  and `rpk … topic consume cdc.olist.orders -o -1 -n 1` shows a new record within 5 s. Record the
  snapshot wall time for all nine tables (`SELECT … FROM pg_replication_slots` confirms slot
  `olist_debezium` active).

- [ ] **Step 3: Restart idempotency.** `make down && make up PROFILE=ingest` → `connect-init exited 0`
  again, no duplicate connector, snapshot **not** repeated (offsets persisted in the
  `CONNECT_OFFSET_STORAGE_TOPIC` on Redpanda's volume).

- [ ] **Step 4: Commit** `feat(ingestion): debezium olist connector registered by connect-init`.

---

### Task 3: `replayer live` — time-compressed replay with realistic updates

**Owner:** `data-engineer`. Independent of T1/T2 (needs only `core`). Exception granted: add the
`replayer` service to `docker-compose.yml` (profile `ingest`, `build: {context: ., dockerfile: ingestion/replayer/Dockerfile}`,
mounts `${OLIST_DATA_DIR}:/data/olist:ro`, env `OLIST_DATA_DIR=/data/olist`,
`POSTGRES_DSN=postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}`,
`REPLAY_SPEED`, command `live --loop`, limit `512m`, `depends_on: postgres: service_healthy`,
`restart: unless-stopped`).

**Files:**
- Create: `ingestion/replayer/src/replayer/timeline.py`, `ingestion/replayer/src/replayer/live.py`,
  `ingestion/replayer/tests/test_timeline.py`, `ingestion/replayer/tests/test_live.py`,
  `ingestion/replayer/Dockerfile`
- Modify: `ingestion/replayer/src/replayer/cli.py`, `docker-compose.yml` (service `replayer` only)

**Interfaces:**
- Produces:
  ```python
  # timeline.py
  @dataclass(frozen=True, order=True)
  class Change:
      ts: datetime            # dataset time, naive UTC-less like Olist
      seq: int                # tie-breaker preserving generation order
      table: str              # olist table name
      key: tuple[tuple[str, object], ...]     # PK columns, sorted by name
      values: tuple[tuple[str, object], ...]  # non-key columns to set, sorted by name

  def build_timeline(data_dir: Path, start: datetime | None, until: datetime | None) -> list[Change]
  def shift_history(changes: list[Change], span: timedelta, iteration: int) -> list[Change]
  REFERENCE_TABLES: tuple[str, ...] = ("product_category_name_translation", "products", "sellers", "geolocation")

  # live.py
  def apply_change(conn: psycopg.Connection, change: Change) -> None   # INSERT … ON CONFLICT (pk) DO UPDATE SET …
  def ensure_reference_tables(conn: psycopg.Connection, data_dir: Path) -> None  # COPY each REFERENCE_TABLES table if count == 0
  def run_live(dsn: str, data_dir: Path, *, start: datetime | None, until: datetime | None,
               speed: float, loop: bool, sleep: Callable[[float], None] = time.sleep) -> dict[str, int]
      # returns changes applied per table; sleeps (next.ts - prev.ts)/speed between changes, never negative
  ```
  CLI: `replayer live [--from ISO] [--until ISO] [--speed F] [--loop] [--summary-file PATH]`;
  `--speed` defaults to env `REPLAY_SPEED` (default 3600); prints `<table>: <n>` lines at exit and,
  with `--summary-file`, writes the same dict as JSON. `--loop` continues past `until` with
  `shift_history` iterations 1, 2, … until killed (SIGINT/SIGTERM → write summary, exit 0).
- Timeline rules (per order, from the CSVs; `ts` strings parsed with polars):
  - at `order_purchase_timestamp`: `customers` upsert (that order's customer row), `orders` upsert
    with `order_status='created'`, all `order_approved_at`/delivery columns **null**, estimated date
    set; every `order_items` row for the order (seq after the order).
  - at `order_approved_at` (if not null): `orders` update `order_status='approved', order_approved_at=…`;
    all `order_payments` rows.
  - at `order_delivered_carrier_date` (if not null): `order_status='shipped', order_delivered_carrier_date=…`.
  - at `order_delivered_customer_date` (if not null): `order_status='delivered', order_delivered_customer_date=…`.
  - final: if the CSV `order_status` differs from the last status emitted, one more `orders` update
    to the CSV status at the max non-null timestamp of the order.
  - `order_reviews`: insert at `review_creation_date` with `review_answer_timestamp` null; update
    setting `review_answer_timestamp` at that time if not null. Key for reviews is
    `(review_id, order_id)` — NOTE `olist.order_reviews` has an identity PK `review_pk`; add a
    `UNIQUE (review_id, order_id)` constraint to `olist_schema.sql` (the Olist duplicates are
    duplicate `review_id` across *different* orders, so this stays satisfiable) and upsert on it.
  - Per-order `ts` values are sorted non-decreasing before emission: if a later step's timestamp is
    earlier than a previous step's (Olist has these), it takes the previous step's `ts`.
  - `start`/`until` filter on `order_purchase_timestamp`; steps after `until` are still emitted
    (they belong to included orders).
  - `shift_history(changes, span, i)`: every `ts += span * i`; `order_id`, `customer_id`,
    `customer_unique_id`, `review_id` replaced by `hashlib.sha1(f"{old}:{i}".encode()).hexdigest()[:32]`
    (Olist ids are 32 hex chars) so loops never collide with real keys.

- [ ] **Step 1: Failing tests in `test_timeline.py`** against the existing `FIXTURE` CSV writer in
  `conftest.py` (extend the fixture with one order having null `order_approved_at` but delivered
  dates, one canceled order, and one review with an answer):
  ```python
  def test_created_before_items_before_approved(
      tmp_data_dir,
  ): ...  # order o1: statuses in order created→approved→shipped→delivered, items seq > order seq
  def test_null_approved_at_skips_step(
      tmp_data_dir,
  ): ...  # no 'approved' change; 'shipped' present; ts non-decreasing
  def test_final_status_applied_when_not_reached(
      tmp_data_dir,
  ): ...  # canceled order ends with order_status == 'canceled'
  def test_review_answer_is_separate_update(tmp_data_dir): ...
  def test_window_filters_on_purchase(
      tmp_data_dir,
  ): ...  # until excludes o2, includes o1's post-until steps
  def test_timeline_is_sorted_and_deterministic(
      tmp_data_dir,
  ): ...  # list(sorted(changes)) == changes; two builds equal
  def test_shift_history_rewrites_ids_and_times(): ...  # ids 32 hex, differ from originals, ts shifted by span*2
  ```
- [ ] **Step 2: Run** `uv run pytest ingestion/replayer/tests/test_timeline.py -v` → FAIL (import error).
- [ ] **Step 3: Implement `timeline.py`.**
- [ ] **Step 4: Run again** → PASS.
- [ ] **Step 5: Failing tests in `test_live.py`** (`@pytest.mark.integration`, use the existing
  throwaway-database fixture):
  ```python
  def test_apply_change_upsert_is_idempotent(
      conn,
  ): ...  # apply same Change twice: one row, values equal, updated_at advanced on 2nd
  def test_run_live_applies_all_changes_fast(
      test_dsn, tmp_data_dir
  ): ...  # speed=1e9, sleep=recorder; counts per table match timeline; sum(sleeps) ≈ span/speed
  def test_run_live_twice_same_window_no_errors(
      test_dsn, tmp_data_dir
  ): ...  # Review Focus 4: second run returns same counts, no exception, row counts unchanged
  def test_reference_tables_loaded_once(test_dsn, tmp_data_dir): ...
  ```
- [ ] **Step 6: Run** → FAIL. **Step 7: Implement `live.py`, CLI, schema constraint.**
  `run_live` opens one connection, autocommit per change (each change = one transaction so CDC sees
  one event per change), handles SIGTERM via `signal.signal` setting a stop flag.
- [ ] **Step 8: Run** `make test && make test-integration` → PASS (existing tests included).
- [ ] **Step 9: Dockerfile + compose service.** `ingestion/replayer/Dockerfile`:
  `FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim@sha256:…`; copy `pyproject.toml uv.lock .python-version ingestion/replayer`;
  `uv sync --locked --no-dev --package replayer`; `ENTRYPOINT ["uv", "run", "--no-sync", "replayer"]`.
  Verify: `docker compose --profile ingest build replayer` and
  `docker compose --profile ingest run --rm replayer status` prints the nine counts.
- [ ] **Step 10: Manual smoke on real data** (core up, full seed): `uv run --package replayer replayer live --from 2017-10-02 --until 2017-10-03 --speed 7200`
  finishes in ≈12 s and prints per-table counts (orders ≈ 2× daily order count because of status
  updates); `make status` orders count unchanged (upserts). Also `make seed` on the full dataset
  must still succeed with the new `UNIQUE (review_id, order_id)` constraint; if the real data
  violates it, stop and report the offending pairs instead of weakening the constraint.
- [ ] **Step 11: Commit** `feat(ingestion): replayer live mode with compressed clock and history loop`.

---

### Task 4: Clickstream session generation and Avro schemas (pure logic)

**Owner:** `data-engineer`. Independent. Creates the package; root `pyproject.toml` workspace glob
`ingestion/*` already includes it.

**Files:**
- Create: `ingestion/clickstream_sim/pyproject.toml` (name `clickstream-sim`, deps
  `confluent-kafka[avro]`, `psycopg[binary]`, `python-dotenv`; script `clickstream-sim = "clickstream_sim.cli:main"`),
  `ingestion/clickstream_sim/src/clickstream_sim/{__init__,events,faults}.py`,
  `ingestion/clickstream_sim/tests/{test_events,test_faults,test_schemas}.py`,
  `ingestion/schemas/events/clickstream_event.v1.avsc`, `ingestion/schemas/events/clickstream_event.v2.avsc`

**Interfaces:**
- Produces:
  ```python
  EVENT_TYPES = ("page_view", "search", "product_view", "add_to_cart", "checkout_started")
  DEVICES = ("mobile", "desktop", "tablet"); REFERRERS = ("direct", "google", "instagram", "email", None)

  @dataclass(frozen=True)
  class Event:
      event_id: str | None; event_type: str; session_id: str | None; customer_id: str | None
      device: str; referrer: str | None; event_ts: str   # ISO-8601 'YYYY-MM-DDTHH:MM:SS' dataset time
      product_id: str | None; search_query: str | None; quantity: int | None; order_id: str | None
      utm_campaign: str | None = None                     # only serialised under schema v2
      def to_dict(self, schema_version: int) -> dict[str, object]

  @dataclass(frozen=True)
  class Catalogue:            # built from olist.products
      by_category: dict[str, tuple[str, ...]]   # category -> product_ids
      category_of: dict[str, str]
      def random_product(self, rng, category: str | None) -> str

  @dataclass(frozen=True)
  class OrderRef:
      order_id: str; customer_id: str | None; purchase_ts: datetime; product_ids: tuple[str, ...]

  def converting_session(order: OrderRef, catalogue: Catalogue, rng: random.Random) -> list[Event]
      # 1 page_view → 0-1 search → 2-5 product_view (same category as a purchased product, the last one being the purchased product)
      # → add_to_cart per purchased product (quantity 1) → checkout_started with order_id; all event_ts in
      # [purchase_ts - 30 min, purchase_ts], strictly increasing; session_id = sha1(order_id)[:16]; event_id = uuid4 from rng
  def browsing_session(at: datetime, catalogue: Catalogue, rng: random.Random) -> list[Event]
      # non-converting: page_view → 0-1 search → 1-4 product_view → 0-1 add_to_cart, no checkout, customer_id None

  # faults.py
  @dataclass(frozen=True)
  class FaultConfig:
      late_rate: float = 0.05; late_max_hours: int = 48; dup_rate: float = 0.01; bad_rate: float = 0.0
  @dataclass(frozen=True)
  class Emission:
      event: Event; delay_s: float; is_duplicate: bool; schema_version: int
  def inject(events: list[Event], cfg: FaultConfig, rng: random.Random, schema_version: int) -> list[Emission]
      # late: delay_s = uniform(1h, late_max_hours) in dataset seconds (caller divides by speed); event unchanged
      # dup: a second Emission of the same event, delay_s = original.delay_s + 1, is_duplicate=True
      # bad: exactly one corruption chosen uniformly: event_id=None | session_id=None | event_ts='not-a-timestamp'
      #      | event_ts='1970-01-01T00:00:00' | event_ts='2099-12-31T00:00:00' | quantity=-1 (quantity only on add_to_cart)
      # rng is the only source of randomness; same inputs → same output
  ```
  Avro v1 schema: record `ClickstreamEvent`, namespace `retail.events`, fields exactly the `Event`
  fields except `utm_campaign`, nullable ones as `["null", "string"]`/`["null","int"]` with
  `"default": null`; `event_ts` is `string`. v2 = v1 + `utm_campaign: ["null","string"], default null`
  (backward compatible so the registry accepts it under the default compatibility level).

- [ ] **Step 1: Failing tests.** `test_events.py`: `test_converting_session_ends_in_checkout_with_order_id`,
  `test_converting_session_ts_strictly_increasing_within_30min`,
  `test_product_views_share_category_and_end_on_purchased_product`,
  `test_browsing_session_has_no_checkout_and_no_customer`, `test_same_seed_same_events`,
  `test_to_dict_v1_has_no_utm_and_matches_schema_fields` (compare keys to v1 `.avsc` field names).
  `test_faults.py`: `test_inject_rates_with_fixed_seed` (1000 events, cfg late .1/dup .1/bad .1 →
  counts within ±30 % of expectation; duplicates reference identical event objects),
  `test_bad_event_has_exactly_one_corruption`, `test_zero_rates_identity`.
  `test_schemas.py`: both `.avsc` parse with `fastavro.parse_schema` (fastavro comes with
  `confluent-kafka[avro]`); v2 fields ⊇ v1 fields; every `Event` field name appears in v2.
- [ ] **Step 2: Run** `uv sync --all-packages --group dev && uv run pytest ingestion/clickstream_sim -v` → FAIL.
- [ ] **Step 3: Implement** `events.py`, `faults.py`, the two `.avsc` files.
- [ ] **Step 4: Run** → PASS; `make lint` passes (add `uv run mypy ingestion` already covers it).
- [ ] **Step 5: Commit** `feat(ingestion): clickstream session generator, fault injection and avro schemas`.

---

### Task 5: Spark bronze app — Avro decode per schema id, dedupe, quarantine, Delta sinks

**Owner:** `data-engineer`. Depends on T1 (spark image, redpanda). Uses T2's topics for the manual
smoke only; unit tests are self-contained.

**Files:**
- Create: `lakehouse/spark/pyproject.toml` (name `lakehouse-spark`, deps `pyspark==<pinned>`,
  `delta-spark==<pinned>`, `deltalake`; script `bronze = "lakehouse_spark.cli:main"` — `cli.py`
  itself is T7), `lakehouse/spark/src/lakehouse_spark/{__init__,wire,registry}.py`,
  `lakehouse/spark/src/lakehouse_spark/bronze/{__init__,decode,sink,app}.py`,
  `lakehouse/spark/tests/{conftest,test_wire,test_registry,test_decode,test_dedupe}.py`
- Modify: root `pyproject.toml` (`members = ["ingestion/*", "lakehouse/spark"]`, `testpaths` add
  `lakehouse`, markers add `spark: needs a SparkSession (run inside the spark container)` and
  `ingest: needs the ingest profile and reseeds the database`), `Makefile` `lint` adds `mypy lakehouse`.

**Interfaces:**
- Consumes: Kafka records from `cdc.olist.*` and `events.*` with registry wire format
  (`0x00` + 4-byte big-endian schema id + Avro binary); Schema Registry REST
  `GET /schemas/ids/{id}` → `{"schema": "<json>"}`.
- Produces:
  ```python
  # wire.py (pure)
  MAGIC = 0
  def parse_wire_header(value: bytes | None) -> int | None   # schema id, or None if value is None/len<5/value[0]!=MAGIC

  # registry.py
  class SchemaNotFound(Exception): ...
  class SchemaRegistry:
      def __init__(self, base_url: str) -> None
      def get(self, schema_id: int) -> str      # Avro schema JSON; cached; raises SchemaNotFound on HTTP 404,
                                                # re-raises URLError/other HTTP errors unchanged (Review Focus 3)

  # bronze/decode.py
  QUARANTINE_REASONS = ("null_payload", "not_wire_format", "unknown_schema_id", "avro_decode_failed",
                        "null_primary_key", "unparseable_timestamp")
  def with_dedupe_key(df: DataFrame) -> DataFrame   # dedupe_key = coalesce(key as string, concat(topic,'-',partition,'-',offset))
  def decode_batch(batch: DataFrame, registry: SchemaRegistry, source: str) -> tuple[DataFrame, DataFrame]
      # batch: raw Kafka columns (key, value, topic, partition, offset, timestamp)
      # returns (good, quarantine). good has decoded columns (from_avro(substring(value,6), schema, {"mode":"PERMISSIVE"}))
      # plus kafka_topic, kafka_partition, kafka_offset, kafka_timestamp, schema_id, ingest_ts, ingest_date.
      # Rows are decoded per distinct schema_id and unioned with unionByName(allowMissingColumns=True).
      # source == "events": null event_id or session_id → null_primary_key; to_timestamp(event_ts, "yyyy-MM-dd'T'HH:mm:ss")
      #   null, or year < 2016, or > current_timestamp + 1 day → unparseable_timestamp. event_ts stays a string column.
      # source == "olist": after IS NULL AND before IS NULL → null_payload.
      # quarantine columns: kafka_topic, kafka_partition, kafka_offset, kafka_timestamp, schema_id (nullable),
      #   raw_value (binary), reason, ingest_ts, ingest_date.

  # bronze/sink.py
  def bronze_path(root: str, source: str, topic: str) -> str   # cdc.olist.orders → f"{root}/bronze/olist/orders"; events.page_view → f"{root}/bronze/events/page_view"
  def write_bronze(good: DataFrame, root: str, source: str) -> None       # per kafka_topic: append, partitionBy ingest_date, mergeSchema true
  def write_quarantine(bad: DataFrame, root: str, source: str) -> None    # f"{root}/bronze/_quarantine/{source}", append, partitionBy ingest_date

  # bronze/app.py
  def build_session(app_name: str = "bronze") -> SparkSession
  def start_query(spark, *, name: str, subscribe_pattern: str, source: str, root: str, registry: SchemaRegistry,
                  bootstrap: str, trigger_seconds: int = 5, max_offsets: int = 50_000, dedupe: bool) -> StreamingQuery
      # readStream kafka, startingOffsets earliest, failOnDataLoss false; if dedupe:
      # withWatermark("timestamp","48 hours").dropDuplicates(["dedupe_key"]) after with_dedupe_key;
      # foreachBatch → decode_batch → write_bronze + write_quarantine; checkpoint f"{root}/_checkpoints/bronze/{name}"
  def main() -> None   # root = f"s3a://{LAKEHOUSE_BUCKET}"; cdc_to_bronze (pattern r"cdc\.olist\..*", source "olist", dedupe False)
                       # and events_to_bronze (pattern r"events\..*", source "events", dedupe True); awaitAnyTermination
  ```
  Bronze CDC row columns: `op, ts_ms, before (struct), after (struct), source (struct)` + kafka
  metadata columns above. Bronze event row columns: the Avro fields (v2 adds `utm_campaign`) +
  metadata columns.

- [ ] **Step 1: Host-side failing tests** (`test_wire.py`, `test_registry.py`, no marker):
  `parse_wire_header` on `None`, `b"\x00\x00\x00"`, `b"\x01" + 4 bytes`, `b"\x00\x00\x00\x00\x07abc"` → `None, None, None, 7`;
  registry: using `http.server` in a thread serving `/schemas/ids/1` → 200 JSON, `/schemas/ids/2` → 404,
  assert `get(1)` returns the schema string, second call does not hit the server (counter), `get(2)`
  raises `SchemaNotFound`, and a registry on a closed port raises `URLError` (not `SchemaNotFound`).
- [ ] **Step 2: Run** `uv run pytest lakehouse -m "not spark" -v` → FAIL. **Step 3: implement, run → PASS.**
- [ ] **Step 4: Container failing tests** (`@pytest.mark.spark`; `conftest.py` builds a
  `local[1]` SparkSession with Delta configured, `tmp_path` as `root` with `file://` paths).
  Build test records with `fastavro.schemaless_writer` and a hand-rolled wire header; a
  `FakeRegistry` dict-backed object with the same `get` contract.
  `test_decode.py`: `test_good_event_decoded_with_metadata`, `test_two_schema_ids_union_adds_utm_column`
  (v1 and v2 records in one batch → good has `utm_campaign`, null for v1 rows),
  `test_null_value_quarantined_as_null_payload` (Review Focus 1), `test_plain_text_quarantined_as_not_wire_format` (RF 2),
  `test_unknown_schema_id_quarantined` (registry returns SchemaNotFound for id 99) and
  `test_registry_connection_error_propagates` (FakeRegistry raising `URLError` → `decode_batch` raises; RF 3),
  `test_null_event_id_quarantined_null_primary_key`, `test_bad_timestamp_quarantined` (three variants),
  `test_cdc_null_before_and_after_quarantined`, `test_counts_add_up` (good + bad == input, for a
  mixed batch), `test_write_bronze_routes_by_topic_and_partitions_by_ingest_date` (read back with
  `spark.read.format("delta")`, two topics → two paths, `ingest_date` partition dir present),
  `test_write_bronze_merge_schema` (write v1 rows then v2 rows to same path → column added).
  `test_dedupe.py`: `test_with_dedupe_key_uses_key_or_offset_triple` (null keys get distinct keys;
  equal keys equal) and a `MemoryStream`-based test: two records with same key within watermark →
  one survives; a third with same key but timestamp 3 days later than the max seen → survives.
- [ ] **Step 5: Run** `make test-spark` → FAIL. **Step 6: Implement `decode.py`, `sink.py`, `app.py`.**
  **Step 7: Run** `make test-spark` → PASS; `make lint` PASS.
- [ ] **Step 8: Smoke against real topics** (requires T2 done; ingest profile up, full seed):
  `docker compose --profile ingest restart spark`; within ~3 min `docker compose logs spark | grep -c 'Batch'` grows;
  `uv run --package lakehouse-spark python -c "from deltalake import DeltaTable; ..."` (or
  `rclone`/`mc ls`) shows `bronze/olist/<table>/_delta_log` for all nine tables; row count of
  `bronze/olist/orders` == 99441 after the snapshot drains. Spark UI at `127.0.0.1:4040` shows two
  active streaming queries. Report `docker stats` for `spark`.
- [ ] **Step 9: Commit** `feat(lakehouse): spark bronze app with avro decode, dedupe and quarantine`.

---

### Task 6: Clickstream producer/consumer wiring and container

**Owner:** `data-engineer`. Depends on T1, T2 (CDC topics), T4. Exception granted: add the
`clickstream-sim` service to `docker-compose.yml`.

**Files:**
- Create: `ingestion/clickstream_sim/src/clickstream_sim/{producer,cdc,cli}.py`,
  `ingestion/clickstream_sim/tests/{test_producer,test_cli}.py`, `ingestion/clickstream_sim/Dockerfile`
- Modify: `docker-compose.yml` (service `clickstream-sim`: build like replayer, env
  `KAFKA_BOOTSTRAP=redpanda:9092`, `SCHEMA_REGISTRY_URL=http://redpanda:8081`, `POSTGRES_DSN` (container form),
  `REPLAY_SPEED`, `SIM_*` passthrough, command `run`, limit `512m`, depends on redpanda healthy and
  `connect-init` completed successfully, `restart: unless-stopped`), `.env.example` (`SIM_LATE_RATE=0.05`,
  `SIM_LATE_MAX_HOURS=48`, `SIM_DUP_RATE=0.01`, `SIM_BAD_RATE=0.0`, `SIM_SCHEMA_EVOLVE_AFTER=0`,
  `SIM_BROWSING_RATIO=3`, `SIM_SEED=42` — `.env.example` is platform-owned; exception granted for
  exactly these lines).

**Interfaces:**
- Consumes: T4's `converting_session`, `browsing_session`, `inject`, `Catalogue`, `OrderRef`;
  T2's topics `cdc.olist.orders`, `cdc.olist.order_items` (Avro, Debezium envelope; the sim reads
  `after.order_id`, `after.customer_id`, `after.order_purchase_timestamp` (ms epoch),
  `after.product_id`); `olist.products` via `POSTGRES_DSN` for the `Catalogue`.
- Produces:
  ```python
  # producer.py
  class EventProducer:
      def __init__(self, bootstrap: str, registry_url: str, schemas_dir: Path) -> None
          # registers v1 under every "events.<type>-value" subject at start; v2 lazily on first use
      def produce(self, em: Emission) -> None
          # topic f"events.{em.event.event_type}", key = event.event_id (None stays None),
          # value = AvroSerializer(schema v{em.schema_version}).serialize(event.to_dict(em.schema_version)),
          # timestamp = int(parse(event.event_ts) * 1000) if event_ts parses else current time
      def flush(self) -> int
  def topic_for(event_type: str) -> str

  # cdc.py
  class OrderFeed:                        # consumer group "clickstream-sim"
      def __init__(self, bootstrap: str, registry_url: str) -> None
      def poll(self, timeout_s: float) -> OrderRef | None
          # keeps pending orders {order_id: (customer_id, purchase_ts)}; emits an OrderRef on the FIRST order_items 'c'/'r'
          # for an order (customer_id None if the order record was never seen); ignores other ops and later items
          # of the same order (keeps a bounded set of 50_000 seen order_ids)

  # cli.py
  @dataclass(frozen=True)
  class SimConfig: ...                     # from env SIM_* + REPLAY_SPEED + KAFKA_BOOTSTRAP + SCHEMA_REGISTRY_URL + POSTGRES_DSN
  def run(cfg: SimConfig, *, max_events: int | None, summary_file: Path | None, sleep=time.sleep) -> dict[str, int]
      # loop: OrderFeed.poll → converting_session; per order also cfg.browsing_ratio browsing sessions at purchase_ts ± 1h;
      # inject → schedule emissions in a heap by wall time (= now + delay_s / speed); produce when due.
      # schema_version flips 1→2 once events_generated >= SIM_SCHEMA_EVOLVE_AFTER > 0.
      # summary: {"events_unique": n, "duplicates": d, "bad": b, "late": l, "produced": n+d, "schema_v2": k}
      # exits when max_events unique events produced and heap drained, or on SIGINT/SIGTERM.
  ```
  CLI: `clickstream-sim run [--max-events N] [--summary-file PATH]`.

- [ ] **Step 1: Failing tests.** `test_producer.py` (no broker: use `confluent_kafka.schema_registry.avro.AvroSerializer`
  with a `MockSchemaRegistryClient` if available in the installed version, else a minimal fake with
  `register_schema`/`get_latest_version`): serialized bytes start with `b"\x00"` and decode with
  `fastavro.schemaless_reader` back to `to_dict`; `topic_for("add_to_cart") == "events.add_to_cart"`;
  Kafka timestamp for `event_ts='2017-10-02T10:00:00'` equals that instant in ms (UTC).
  `test_cli.py`: `run()` with a fake `OrderFeed` yielding 3 orders and a fake producer collecting
  emissions, `speed=1e9`, `max_events=…`: summary adds up (`produced == events_unique + duplicates`),
  emissions ordered by due time, schema flips to 2 after `SIM_SCHEMA_EVOLVE_AFTER`.
- [ ] **Step 2: Run** → FAIL. **Step 3: implement.** **Step 4: run** `uv run pytest ingestion/clickstream_sim` → PASS, `make lint` PASS.
- [ ] **Step 5: Dockerfile** (same shape as replayer's, `--package clickstream-sim`), compose service,
  `.env.example` lines.
- [ ] **Step 6: Smoke** (ingest up, full seed, T5 running): in one shell
  `uv run --package replayer replayer live --from 2017-10-02 --until 2017-10-03 --speed 600`; in another
  `SIM_BAD_RATE=0.05 SIM_SCHEMA_EVOLVE_AFTER=500 uv run --package clickstream-sim clickstream-sim run --max-events 2000 --summary-file /tmp/sim.json`.
  Expect: five `events.*` topics in Console with ten subjects (`v1`, `v2` versions under each);
  `/tmp/sim.json` summary; after ~1 min `bronze/events/*` and `bronze/_quarantine/events` exist and
  `sum(bronze events) + quarantine == events_unique` (use `deltalake` from the host; `make status`
  from T7 is not available yet — a one-liner is fine). Report the numbers.
- [ ] **Step 7: Commit** `feat(ingestion): clickstream-sim producer, cdc order feed and container`.

---

### Task 7: `bronze status`, Make targets, end-to-end `ingest` integration test

**Owner:** `data-engineer`. Depends on T2, T3, T5, T6. Exception granted: `Makefile` targets
`replay`, `sim`, `test-ingest`, and extending `status`.

**Files:**
- Create: `lakehouse/spark/src/lakehouse_spark/cli.py`, `lakehouse/spark/tests/test_cli.py`,
  `tests/__init__.py` (empty), `tests/integration/__init__.py`, `tests/integration/test_ingest.py`
- Modify: `Makefile`, root `pyproject.toml` (dev group adds `deltalake`, `psycopg[binary]`,
  `python-dotenv`; HTTP calls use `urllib`; `testpaths` add `tests`).

**Interfaces:**
- `bronze status [--root s3://lakehouse] [--json]`: reads every Delta table under `bronze/olist/*`,
  `bronze/events/*`, `bronze/_quarantine/*` with `deltalake.DeltaTable(path, storage_options={endpoint, keys, allow_http})`
  built from `MINIO_*` env + `MINIO_ENDPOINT` (default `http://127.0.0.1:9000`); prints
  `bronze/olist/orders: 99441`, …, `quarantine/events: 12`, one per line, sorted; missing tables
  print `: 0`. `--json` dumps `{"bronze/olist/orders": 99441, ...}`. Quarantine `reason` histogram
  printed under `quarantine/<source> reasons: {...}`.
- Make: `status` = `replayer status` then `bronze status`; `replay` = `uv run --package replayer replayer live $(REPLAY_ARGS)`;
  `sim` = `uv run --package clickstream-sim clickstream-sim run $(SIM_ARGS)`;
  `test-ingest` = `uv run pytest -m ingest tests/integration` with a leading
  `@echo "WARNING: reseeds $$POSTGRES_DB with the sample fixture and clears bronze; set ALLOW_RESEED=1 to proceed"`.
- Integration test (`@pytest.mark.ingest`, module-level `pytest.skip` unless `ALLOW_RESEED=1` and
  Kafka Connect reachable), in order:
  1. `seed(dsn, Path("tests/fixtures/olist_sample"))`; clear bronze and checkpoints with
     `subprocess.run(["docker", "compose", "exec", "-T", "minio", "mc", "rm", "-r", "--force", f"local/{bucket}/bronze", f"local/{bucket}/_checkpoints"])`
     (the `minio` container's `mc` already has the `local` alias from `minio-init`; if not, set it
     first in the same command); drop and re-register the connector (`DELETE /connectors/olist-postgres`
     via `urllib`, then `docker compose --profile ingest run --rm connect-init`) so a fresh snapshot
     runs; `docker compose restart spark` so the queries restart from empty checkpoints.
  2. Poll `bronze status --json` every 5 s (max 240 s) until for all nine tables
     `bronze/olist/<t> + quarantine_rows_for_topic(t) == sample row count`; assert quarantine for
     `olist` is 0.
  3. Run `replayer live --from <min purchase in fixture> --until <max purchase> --speed 1e6 --summary-file`
     via `subprocess` on the host; then poll until `bronze/olist/<t> == snapshot + live_changes[t]`
     for every table in the summary (max 240 s).
  4. Run `clickstream-sim run --max-events 300 --summary-file` with env
     `SIM_BAD_RATE=0.1 SIM_DUP_RATE=0.1 SIM_LATE_RATE=0.2 SIM_SCHEMA_EVOLVE_AFTER=100 REPLAY_SPEED=1e6`;
     poll until `sum(bronze/events/*) + quarantine/events == summary.events_unique` (max 240 s);
     assert reasons ⊇ `{"null_primary_key", "unparseable_timestamp"}`; assert `utm_campaign` is a
     column of at least one `bronze/events/*` table (read schema with `deltalake`).
  Each poll failure prints the latest `bronze status` output and the last 50 lines of
  `docker compose logs spark` on timeout.

- [ ] **Step 1: `test_cli.py`** (host, marker none): with a `tmp_path` Delta table written via
  `deltalake.write_deltalake`, `bronze status --root file://… --json` lists it and reports `0` for a
  missing one; reasons histogram computed from a quarantine table with two reasons.
- [ ] **Step 2: Run → FAIL; implement `cli.py`; run → PASS.**
- [ ] **Step 3: Write `test_ingest.py`** per Interfaces; Make targets.
- [ ] **Step 4: Run locally**: `ALLOW_RESEED=1 make test-ingest` → 1 passed (report wall time and the
  final `bronze status`). Then restore your dev DB: `make seed`, and let the connector re-snapshot
  (or accept the sample state; say which).
- [ ] **Step 5: Commit** `feat(ingestion): bronze status, make replay/sim and ingest integration test`.

---

### Task 8: CI `ingest` job, ADR-0003, runbook, README

**Owner:** `platform-engineer`. Depends on T7.

**Files:**
- Modify: `.github/workflows/ci.yml`, `README.md`, `docs/runbooks/windows-setup.md` (memory note)
- Create: `docs/adr/0003-ingestion-serialization.md`, `docs/runbooks/ingestion.md`

**Interfaces:** none new.

- [ ] **Step 1: CI job `ingest`** (`needs: lint-test`, `timeout-minutes: 30`): checkout, setup-uv,
  `uv sync --locked --all-packages --group dev`, `docker/setup-buildx-action`, build `kafka-connect`,
  `spark`, `replayer`, `clickstream-sim` images with `docker/bake-action` or
  `docker compose --profile ingest build` using `cache-from/cache-to: type=gha` (one cache scope per
  image), `cp .env.example .env && echo OLIST_DATA_DIR=tests/fixtures/olist_sample >> .env`,
  `make up PROFILE=ingest`, `make test-spark`, `ALLOW_RESEED=1 make test-ingest`, on failure
  `docker compose --profile ingest logs --tail 100`, always `make down`. Keep the existing
  `integration` job unchanged.
- [ ] **Step 2: ADR-0003** (≤ 40 lines): Avro + Schema Registry wire format; why Confluent
  `cp-kafka-connect-base` rather than the Debezium image (Apicurio-only converters, verified in its
  Dockerfile); per-schema-id decoding in `foreachBatch` so evolution adds columns instead of
  misdecoding; Kafka key = `event_id`, timestamp = `event_ts` so watermark dedupe runs before
  decoding; `time.precision.mode=connect`; quarantine reasons list; the 48 h late-duplicate
  limitation. Pinned versions table.
- [ ] **Step 3: `docs/runbooks/ingestion.md`**: start (`make up PROFILE=ingest`), replay
  (`make replay REPLAY_ARGS="--from 2017-10-02 --until 2017-10-03 --speed 7200"`), sim, Console URL
  and what to look at (topics, schemas, connector), Spark UI, `make status` reading, quarantine
  reasons and how to inspect them with `deltalake`, resetting bronze (the `mc rm` + connector
  re-register + spark restart recipe from T7), memory expectations, `make test-ingest` warning.
- [ ] **Step 4: README**: "Run it" gains the ingest profile section and a Phase 1 line in the
  status table; architecture snippet unchanged.
- [ ] **Step 5: Verify**: `yamllint`-free parse via `python -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml'))"`;
  push the branch, open the PR, CI `ingest` job green (iterate on cache/memory until it is; report
  job wall time).
- [ ] **Step 6: Commit** `ci: ingest profile job; docs: ADR-0003, ingestion runbook`.
