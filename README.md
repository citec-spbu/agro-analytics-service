# agro-analytics-service

Микросервис аналитики: синхронизирует данные из PostgreSQL/сервисов в ClickHouse и отдаёт API для дашбордов.

## Стек
- Python 3.11
- FastAPI
- ClickHouse
- Docker / Docker Compose

## Быстрый запуск
```bash
docker network create agronetwork 2>/dev/null || true
cp .env.example .env
docker compose up -d --build
```

HTTP API будет доступен на `http://localhost:8010`, Swagger - `http://localhost:8010/docs`.

## CDC-режим (опционально)
Для запуска с Kafka/Debezium используйте:
```bash
docker compose -f docker-compose.yml -f docker-compose.cdc.yml up -d
./scripts/register-debezium-connector.sh
```

## Переменные окружения
Все параметры описаны в `.env.example`.

Ключевые:
- `APP_SYNC_INTERVAL_SECONDS`
- `APP_CDC_ENABLED`
- `APP_CLICKHOUSE_*`
- `APP_FIELDS_DATABASE_URL`, `APP_METEO_DATABASE_URL`, `APP_DZZ_DATABASE_URL`
