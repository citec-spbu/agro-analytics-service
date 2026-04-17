# agro-analytics-service

Сервис аналитики: витрины в ClickHouse, выгрузка из PostgreSQL (поля / метео / ДЗЗ), HTTP API для дашбордов.

## Поток данных

### Режим по умолчанию (опрос)

Фоновый цикл: **полный снимок** `dim_field` и `fact_crop_rotation` из БД полей + **инкремент** ДЗЗ/метео по watermark в ClickHouse. Интервал `APP_SYNC_INTERVAL_SECONDS`.

### Режим CDC + Kafka (опционально)

Стек: **PostgreSQL logical replication** → **Debezium** (Kafka Connect) → **Apache Kafka** (один брокер, **KRaft**, без ZooKeeper) → потребитель в `analytics-service` с **дебаунсом** пересборки витрин полей/севооборота из PostgreSQL (проверенный снимок, как в `sync.py`, а не построчный UPSERT — достаточно для ВКР и отчётности).

ДЗЗ и метео по-прежнему подтягиваются **таймером** из своих БД.

**Шаги:**

1. В `agro-fields-service` включён `wal_level=logical` (см. `docker-compose.yml`). При смене на уже существующем томе БД может понадобиться пересоздать volume.
2. Поднять поля + ClickHouse + аналитику как обычно, затем CDC-слой:
   ```bash
   docker compose -f docker-compose.yml -f docker-compose.cdc.yml up -d
   ```
3. После миграций Liquibase в fields-db зарегистрировать коннектор (логин/пароль/имя БД в `debezium/connectors/fields-postgres.json` должны совпадать с вашим `.env` fields):
   ```bash
   ./scripts/register-debezium-connector.sh
   ```
4. В `.env` аналитики: `APP_CDC_ENABLED=true` (в `docker-compose.cdc.yml` это уже выставляется через `environment`).

Топики: `fields_cdc.public.seasons|fields|contours|crop_rotations`.

Брокер Kafka в `docker-compose.cdc.yml` слушает **9092** на хосте (если порт занят локальным Kafka — смените маппинг `ports`).

Kafka Connect REST: **8083**. **Kafka UI** (топики, consumer groups, сообщения, коннекторы): **http://localhost:8084** — сервис `kafka-ui` в том же compose.

Для продакшена обычно добавляют отдельного пользователя репликации, публикацию вручную, ACL в Kafka и инкрементальную запись в ClickHouse без полного TRUNCATE.

## Запуск

Сеть Docker: `agronetwork` (как у остальных сервисов).

```bash
docker compose up -d
```

Переменные — см. `.env.example`.

## API

Префикс за gateway: `/api/analytics/...`, JWT через `POST /api/auth/introspect` (как у других сервисов).

## Витрины

В `clickhouse/init/01-schema.sql`: `dim_field`, `fact_dzz_scene`, `fact_meteo_observation`. Агрегаты для отчётов считаются в API по фактам; при росте объёма данных имеет смысл вынести их в `AggregatingMergeTree` / материализованные представления.
