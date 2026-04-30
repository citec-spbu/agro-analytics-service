from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    TITLE: str = "agro-analytics"
    VERSION: str = "0.1.0"
    AUTH_SERVICE_URL: str = "http://auth-service:8080"
    CLICKHOUSE_HOST: str = "clickhouse"
    CLICKHOUSE_PORT: int = 8123
    CLICKHOUSE_USER: str = "default"
    CLICKHOUSE_PASSWORD: str = ""
    CLICKHOUSE_DATABASE: str = "agro_marts"
    SYNC_INTERVAL_SECONDS: float = 15.0
    FIELDS_DATABASE_URL: str = ""
    METEO_DATABASE_URL: str = ""
    DZZ_DATABASE_URL: str = ""
    # CDC flow: Debezium -> Kafka -> consumer rebuilds dim_field + fact_crop_rotation with debounce.
    CDC_ENABLED: bool = False
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:9092"
    KAFKA_FIELDS_TOPIC_PREFIX: str = "fields_cdc"
    KAFKA_CONSUMER_GROUP: str = "analytics-fields-warehouse"
    FIELDS_SYNC_DEBOUNCE_SECONDS: float = 3.0

    model_config = SettingsConfigDict(env_prefix="APP_", env_file=".env", extra="ignore")


settings = Settings()
