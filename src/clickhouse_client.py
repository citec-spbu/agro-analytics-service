import clickhouse_connect

from src.config import settings


def get_client():
    return clickhouse_connect.get_client(
        host=settings.CLICKHOUSE_HOST,
        port=settings.CLICKHOUSE_PORT,
        username=settings.CLICKHOUSE_USER,
        password=settings.CLICKHOUSE_PASSWORD or None,
        database=settings.CLICKHOUSE_DATABASE,
    )
