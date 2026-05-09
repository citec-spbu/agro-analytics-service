import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.clickhouse_bootstrap import ensure_clickhouse_schema
from src.config import settings
from src.kafka_fields_consumer import start_kafka_consumer_background, stop_kafka_consumer
from src.routers import analytics
from src.sync import run_sync_cycle, sync_fields_warehouse

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


async def _fields_full_sync_loop() -> None:
    """Резервная синхронизация полей из PostgreSQL при включённом CDC (см. FIELDS_FULL_SYNC_INTERVAL_SECONDS)."""
    if not settings.CDC_ENABLED or settings.FIELDS_FULL_SYNC_INTERVAL_SECONDS <= 0:
        return
    if not settings.FIELDS_DATABASE_URL:
        return
    await asyncio.sleep(15)
    while True:
        try:
            await asyncio.to_thread(sync_fields_warehouse)
            log.info(
                "Плановая полная синхронизация полей/сезонов в ClickHouse (резерв при CDC, каждые %ss)",
                settings.FIELDS_FULL_SYNC_INTERVAL_SECONDS,
            )
        except Exception:
            log.exception("Резервная синхронизация полей из PostgreSQL не удалась")
        await asyncio.sleep(settings.FIELDS_FULL_SYNC_INTERVAL_SECONDS)


async def _sync_loop() -> None:
    await asyncio.sleep(2)
    while True:
        try:
            # In CDC mode fields come from Kafka; this loop keeps DZZ/meteo data fresh and backfills empty dimensions.
            await asyncio.to_thread(run_sync_cycle)
        except Exception:
            log.exception("sync cycle failed")
        await asyncio.sleep(settings.SYNC_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await asyncio.to_thread(ensure_clickhouse_schema)
    if settings.CDC_ENABLED:
        start_kafka_consumer_background()
    task = asyncio.create_task(_sync_loop())
    full_sync_task = asyncio.create_task(_fields_full_sync_loop())
    yield
    stop_kafka_consumer()
    task.cancel()
    full_sync_task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    try:
        await full_sync_task
    except asyncio.CancelledError:
        pass


app = FastAPI(
    title=settings.TITLE,
    version=settings.VERSION,
    lifespan=lifespan,
    redirect_slashes=False,
)
app.include_router(analytics.router, prefix="/api/analytics")
