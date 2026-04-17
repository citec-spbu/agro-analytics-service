#!/usr/bin/env bash
# Регистрация Debezium-коннектора (PostgreSQL fields → Kafka).
# Требования: fields-db с wal_level=logical, таблицы уже созданы Liquibase, kafka-connect слушает 8083.

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
URL="${CONNECT_REST_URL:-http://localhost:8083}"
JSON="${ROOT}/debezium/connectors/fields-postgres.json"

if [[ ! -f "$JSON" ]]; then
  echo "Не найден $JSON" >&2
  exit 1
fi

echo "POST $URL/connectors"
curl -sfS -X POST -H "Content-Type: application/json" \
  --data @"$JSON" \
  "$URL/connectors" && echo OK || {
  echo "Если коннектор уже есть: curl -s $URL/connectors/fields-pg-cdc | jq ." >&2
  exit 1
}
