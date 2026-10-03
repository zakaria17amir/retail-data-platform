#!/bin/sh
set -eu

CONNECT_URL="${CONNECT_URL:-http://kafka-connect:8083}"
NAME=olist-postgres
CONFIG=/debezium/olist-postgres.json

: "${POSTGRES_USER:?}" "${POSTGRES_PASSWORD:?}" "${POSTGRES_DB:?}"

escape() {
  printf '%s' "$1" | sed -e 's/[\\"]/\\&/g' -e 's/[\\&|]/\\&/g'
}

regex_escape() {
  printf '%s' "$1" | sed -e 's/[][\\|&.*^$]/\\&/g'
}

mask_password() {
  raw=$(regex_escape "$POSTGRES_PASSWORD")
  json=$(regex_escape "$(printf '%s' "$POSTGRES_PASSWORD" | sed -e 's/[\\"]/\\&/g')")
  sed -e "s|$json|***|g" -e "s|$raw|***|g"
}

unknown=$(grep -o '\${[A-Z_]*}' "$CONFIG" \
  | grep -v -e '{POSTGRES_USER}' -e '{POSTGRES_PASSWORD}' -e '{POSTGRES_DB}' || true)
if [ -n "$unknown" ]; then
  echo "unknown placeholder in $CONFIG: $unknown" >&2
  exit 1
fi

deadline=$(($(date +%s) + 120))
until curl -fs --connect-timeout 5 --max-time 10 "$CONNECT_URL/connectors" >/dev/null; do
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

deadline=$(($(date +%s) + 120))
while :; do
  code=$(curl -sS --connect-timeout 5 --max-time 30 -o /tmp/put.out -w '%{http_code}' -X PUT \
    -H 'Content-Type: application/json' --data "$body" \
    "$CONNECT_URL/connectors/$NAME/config" 2>/tmp/put.err) || code=000
  case "$code" in
    200|201) break ;;
    409|5??|000)
      if [ "$(date +%s)" -ge "$deadline" ]; then
        echo "PUT /connectors/$NAME/config still failing with HTTP $code after 120 s:" >&2
        { cat /tmp/put.err /tmp/put.out 2>/dev/null; } | mask_password >&2
        exit 1
      fi
      sleep 3
      ;;
    *)
      echo "PUT /connectors/$NAME/config failed with HTTP $code:" >&2
      { cat /tmp/put.err /tmp/put.out 2>/dev/null; } | mask_password >&2
      exit 1
      ;;
  esac
done

deadline=$(($(date +%s) + 60))
while :; do
  status=$(curl -fs --connect-timeout 5 --max-time 10 "$CONNECT_URL/connectors/$NAME/status" || true)
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
