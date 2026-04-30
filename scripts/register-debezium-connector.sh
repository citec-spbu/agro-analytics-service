#!/usr/bin/env bash
# Register Debezium connector (PostgreSQL fields -> Kafka).
# Requirements: fields-db has wal_level=logical, Liquibase schema is applied, kafka-connect listens on 8083.

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
URL="${CONNECT_REST_URL:-http://localhost:8083}"
JSON="${ROOT}/debezium/connectors/fields-postgres.json"

if [[ ! -f "$JSON" ]]; then
  echo "File not found: $JSON" >&2
  exit 1
fi

echo "POST $URL/connectors"
curl -sfS -X POST -H "Content-Type: application/json" \
  --data @"$JSON" \
  "$URL/connectors" && echo OK || {
  echo "If connector already exists: curl -s $URL/connectors/fields-pg-cdc | jq ." >&2
  exit 1
}
