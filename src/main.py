import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.clickhouse_bootstrap import ensure_clickhouse_schema
from src.config import settings
from src.kafka_fields_consumer import start_kafka_consumer_background, stop_kafka_consumer
from src.routers import analytics
from src.sync import run_sync_cycle

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


async def _sync_loop() -> None:
    await asyncio.sleep(2)
    while True:
        try:
            # При CDC поля обновляются из Kafka; цикл тянет ДЗЗ/метео и подстраховывает пустой dim.
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
    yield
    stop_kafka_consumer()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(
    title=settings.TITLE,
    version=settings.VERSION,
    lifespan=lifespan,
    redirect_slashes=False,
)
app.include_router(analytics.router, prefix="/api/analytics")
