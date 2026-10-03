#!/bin/sh
# Rebuilds the whole ingest path from the current database content:
# stops spark, forgets the Debezium connector/slot, deletes the CDC and event topics, clears
# bronze/ and the streaming checkpoints, reloads the database (seed|seed-sample), re-registers the
# connector (its stored offsets were reset, so the initial snapshot runs again) and restarts spark.
# Usage (repo root, environment from .env): sh ingestion/reset-bronze.sh seed|seed-sample
set -eu

SEED_TARGET="${1:?usage: reset-bronze.sh seed|seed-sample}"
case "$SEED_TARGET" in seed | seed-sample) ;; *) echo "unknown target $SEED_TARGET" >&2; exit 1 ;; esac

: "${POSTGRES_USER:?}" "${POSTGRES_DB:?}" "${LAKEHOUSE_BUCKET:?}"
CONNECT_URL="${KAFKA_CONNECT_URL:-http://127.0.0.1:8083}"
CONNECTOR=olist-postgres
COMPOSE="docker compose --profile ingest"

echo "[1/8] stopping spark"
$COMPOSE stop spark >/dev/null

echo "[2/8] stopping connector $CONNECTOR and resetting its offsets"
if curl -fs "$CONNECT_URL/connectors/$CONNECTOR" >/dev/null; then
  curl -fs -X PUT "$CONNECT_URL/connectors/$CONNECTOR/stop" >/dev/null
  for _ in $(seq 1 30); do
    curl -fs "$CONNECT_URL/connectors/$CONNECTOR/status" | grep -q '"state":"STOPPED"' && break
    sleep 1
  done
  curl -fs -X DELETE "$CONNECT_URL/connectors/$CONNECTOR/offsets" >/dev/null
fi

echo "[3/8] dropping replication slot olist_debezium"
for _ in $(seq 1 30); do
  if $COMPOSE exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc \
    "select pg_drop_replication_slot(slot_name) from pg_replication_slots where slot_name = 'olist_debezium'" \
    >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

echo "[4/8] deleting topics cdc.olist.* and events.*, consumer group clickstream-sim"
$COMPOSE exec -T redpanda rpk topic delete -r '^cdc\.olist\..*' '^events\..*' >/dev/null 2>&1 || true
$COMPOSE exec -T redpanda rpk group delete clickstream-sim >/dev/null 2>&1 || true

echo "[5/8] clearing bronze/ and _checkpoints/ in bucket $LAKEHOUSE_BUCKET"
$COMPOSE exec -T minio sh -c \
  "mc alias set local http://localhost:9000 \"\$MINIO_ROOT_USER\" \"\$MINIO_ROOT_PASSWORD\" >/dev/null \
   && mc rm -r --force local/$LAKEHOUSE_BUCKET/bronze local/$LAKEHOUSE_BUCKET/_checkpoints" \
  >/dev/null 2>&1 || true

echo "[6/8] reloading database ($SEED_TARGET)"
make "$SEED_TARGET" >/dev/null

echo "[7/8] registering connector and waiting for the snapshot"
completed_before=$($COMPOSE logs kafka-connect 2>/dev/null | grep -c "Snapshot completed" || true)
curl -s -X PUT "$CONNECT_URL/connectors/$CONNECTOR/resume" >/dev/null || true
$COMPOSE run --rm connect-init
deadline=$(($(date +%s) + 900))
until [ "$($COMPOSE logs kafka-connect 2>/dev/null | grep -c "Snapshot completed" || true)" -gt "$completed_before" ]; do
  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo "snapshot did not complete within 900 s" >&2
    exit 1
  fi
  sleep 3
done

echo "[8/8] starting spark"
$COMPOSE up -d spark >/dev/null
echo "reset complete"
