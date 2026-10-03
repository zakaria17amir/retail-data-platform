#!/bin/sh
set -eu

CONNECT_URL="${CONNECT_URL:-http://kafka-connect:8083}"
NAME=olist-postgres
CONFIG=/debezium/olist-postgres.json

: "${POSTGRES_USER:?}" "${POSTGRES_PASSWORD:?}" "${POSTGRES_DB:?}"

escape() {
  printf '%s' "$1" | sed -e 's/[\\&|]/\\&/g'
}

deadline=$(($(date +%s) + 120))
until curl -fs "$CONNECT_URL/connectors" >/dev/null; do
  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo "kafka-connect not reachable at $CONNECT_URL" >&2
    exit 1
  fi
  sleep 2
done

body=$(sed \
  -e "s|\${POSTGRES_USER}|$(escape "$POSTGRES_USER")|g" \
  -e "s|\${POSTGRES_PASSWORD}|$(escape "$POSTGRES_PASSWORD")|g" \
  -e "s|\${POSTGRES_DB}|$(escape "$POSTGRES_DB")|g" \
  "$CONFIG")

response=$(curl -s -o /tmp/put.out -w '%{http_code}' -X PUT \
  -H 'Content-Type: application/json' --data "$body" \
  "$CONNECT_URL/connectors/$NAME/config")
case "$response" in
  200|201) ;;
  *)
    echo "PUT /connectors/$NAME/config failed with HTTP $response:" >&2
    cat /tmp/put.out >&2
    exit 1
    ;;
esac

deadline=$(($(date +%s) + 60))
while :; do
  status=$(curl -fs "$CONNECT_URL/connectors/$NAME/status" || true)
  states=$(printf '%s' "$status" | grep -o '"state":"[A-Z]*"' | wc -l)
  running=$(printf '%s' "$status" | grep -o '"state":"RUNNING"' | wc -l)
  if [ "$states" -ge 2 ] && [ "$states" -eq "$running" ]; then
    echo "$NAME is RUNNING ($running connector/task states)"
    exit 0
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo "$NAME did not reach RUNNING within 60 s; status:" >&2
    echo "$status" >&2
    exit 1
  fi
  sleep 2
done
