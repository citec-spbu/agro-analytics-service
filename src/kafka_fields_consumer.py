"""
Потребитель Kafka: события Debezium по таблицам fields-db → дебаунс → пересборка dim_field + fact_crop_rotation.

Реализация для ВКР: не потоковый UPSERT по строкам, а проверенный снимок из PostgreSQL (как в sync.py),
срабатывающий на поток изменений (near-CDC).
"""

from __future__ import annotations

import logging
import threading

from src.config import settings
from src.sync import sync_fields_warehouse

log = logging.getLogger(__name__)

_lock = threading.Lock()
_timer: threading.Timer | None = None
_consumer_thread: threading.Thread | None = None
_stop = threading.Event()


def _debounced_apply() -> None:
    try:
        log.info("CDC debounce: пересборка витрин полей и севооборота в ClickHouse")
        sync_fields_warehouse()
    except Exception:
        log.exception("CDC: ошибка пересборки витрин полей")


def schedule_fields_refresh() -> None:
    global _timer
    with _lock:
        if _timer is not None:
            _timer.cancel()
        delay = max(0.5, float(settings.FIELDS_SYNC_DEBOUNCE_SECONDS))
        _timer = threading.Timer(delay, _debounced_apply)
        _timer.daemon = True
        _timer.start()


def _fields_cdc_topics() -> list[str]:
    p = settings.KAFKA_FIELDS_TOPIC_PREFIX.strip().rstrip(".")
    tables = ("seasons", "fields", "contours", "crop_rotations")
    return [f"{p}.public.{t}" for t in tables]


def _consumer_loop() -> None:
    try:
        from confluent_kafka import Consumer, KafkaError
    except ImportError as e:
        log.error("confluent-kafka не установлен: %s", e)
        return

    conf = {
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": settings.KAFKA_CONSUMER_GROUP,
        "enable.auto.commit": True,
        "auto.offset.reset": "latest",
        "client.id": "analytics-fields-cdc",
    }
    topics = _fields_cdc_topics()
    c = Consumer(conf)
    try:
        c.subscribe(topics)
        log.info("Kafka CDC: подписка на %s", topics)
        while not _stop.is_set():
            msg = c.poll(1.0)
            if msg is None:
                continue
            err = msg.error()
            if err is not None:
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("Kafka poll error: %s", err)
                continue
            schedule_fields_refresh()
    except Exception:
        log.exception("Kafka consumer остановлен из-за ошибки")
    finally:
        try:
            c.close()
        except Exception:
            pass


def start_kafka_consumer_background() -> None:
    global _consumer_thread
    if not settings.CDC_ENABLED:
        return
    if _consumer_thread is not None and _consumer_thread.is_alive():
        return
    _stop.clear()
    _consumer_thread = threading.Thread(target=_consumer_loop, name="kafka-fields-cdc", daemon=True)
    _consumer_thread.start()
    log.info("Фоновый потребитель Kafka (CDC полей) запущен")


def stop_kafka_consumer() -> None:
    _stop.set()
    global _timer
    with _lock:
        if _timer is not None:
            _timer.cancel()
            _timer = None
    if _consumer_thread is not None:
        _consumer_thread.join(timeout=5.0)
