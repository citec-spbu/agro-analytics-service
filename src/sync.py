"""
Инкрементальная выгрузка PostgreSQL → ClickHouse.

Подходит для ВКР и dev: короткий интервал опроса имитирует near-real-time.
Для продакшена тот же контракт данных можно наполнять через Debezium/Kafka
или внешний ETL без изменения схемы витрин.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row

from src.clickhouse_client import get_client
from src.config import settings

log = logging.getLogger(__name__)

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _connect(url: str) -> psycopg.Connection:
    return psycopg.connect(url, row_factory=dict_row)


def _get_watermark(ch, source: str) -> datetime:
    rows = ch.query(
        """
        SELECT coalesce(
            argMax(watermark, updated),
            toDateTime64('1970-01-01 00:00:00', 3, 'UTC')
        )
        FROM sync_cursors
        WHERE source = {src:String}
        """,
        parameters={"src": source},
    ).result_rows
    if not rows or rows[0][0] is None:
        return EPOCH
    v = rows[0][0]
    if isinstance(v, datetime) and v.tzinfo is None:
        return v.replace(tzinfo=UTC)
    return v


def _set_watermark(ch, source: str, watermark: datetime) -> None:
    ch.insert(
        "sync_cursors",
        [[source, watermark, datetime.now(tz=UTC)]],
        column_names=["source", "watermark", "updated"],
    )


def _load_field_org_map(conn: psycopg.Connection) -> dict[str, str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT f.id::text AS field_id, s.organization_id::text AS organization_id
            FROM fields f
            JOIN seasons s ON f.season_id = s.id
            WHERE NOT f.archived AND NOT s.archived
            """
        )
        return {r["field_id"]: r["organization_id"] for r in cur.fetchall()}


def _sync_dim_fields(ch, fields_url: str) -> dict[str, str]:
    if not fields_url:
        return {}
    with _connect(fields_url) as conn:
        mapping = _load_field_org_map(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    f.id::text AS field_id,
                    f.name AS field_name,
                    s.id::text AS season_id,
                    s.name AS season_name,
                    s.organization_id::text AS organization_id,
                    count(c.id)::int AS contour_count,
                    coalesce(
                        sum(ST_Area(c.geom::geography)) / 10000.0,
                        NULL
                    ) AS field_area_ha,
                    avg(ST_Y(ST_Centroid(c.geom::geometry))) AS centroid_lat,
                    avg(ST_X(ST_Centroid(c.geom::geometry))) AS centroid_lon
                FROM fields f
                JOIN seasons s ON f.season_id = s.id
                LEFT JOIN contours c ON c.field_id = f.id AND NOT c.archived
                WHERE NOT f.archived AND NOT s.archived
                GROUP BY f.id, f.name, s.id, s.name, s.organization_id
                """
            )
            rows = cur.fetchall()
    now = datetime.now(tz=UTC)
    batch = [
        [
            r["field_id"],
            r["field_name"],
            r["season_id"],
            r["season_name"],
            r["organization_id"],
            int(r["contour_count"] or 0),
            float(r["field_area_ha"]) if r["field_area_ha"] is not None else None,
            float(r["centroid_lat"]) if r["centroid_lat"] is not None else None,
            float(r["centroid_lon"]) if r["centroid_lon"] is not None else None,
            now,
        ]
        for r in rows
    ]
    ch.command("TRUNCATE TABLE IF EXISTS dim_field")
    if batch:
        ch.insert(
            "dim_field",
            batch,
            column_names=[
                "field_id",
                "field_name",
                "season_id",
                "season_name",
                "organization_id",
                "contour_count",
                "field_area_ha",
                "centroid_lat",
                "centroid_lon",
                "synced_at",
            ],
        )
    log.info("dim_field synced: %s rows", len(batch))
    return mapping


def _sync_crop_rotations(ch, fields_url: str) -> None:
    if not fields_url:
        return
    with _connect(fields_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    cr.id::text AS crop_rotation_id,
                    s.organization_id::text AS organization_id,
                    f.id::text AS field_id,
                    f.name AS field_name,
                    s.id::text AS season_id,
                    s.name AS season_name,
                    c.id::text AS contour_id,
                    c.name AS contour_name,
                    cr.culture,
                    cr.cultivar,
                    cr.start_date,
                    cr.end_date,
                    cr.description,
                    cr.harvest_yield_t_per_ha,
                    CASE
                        WHEN c.geom IS NOT NULL THEN
                            ST_Area(c.geom::geography) / 10000.0
                        ELSE NULL
                    END AS contour_area_ha
                FROM crop_rotations cr
                JOIN contours c ON c.id = cr.contour_id
                JOIN fields f ON f.id = c.field_id
                JOIN seasons s ON s.id = f.season_id
                WHERE NOT cr.archived
                  AND NOT c.archived
                  AND NOT f.archived
                  AND NOT s.archived
                """
            )
            rows = cur.fetchall()
    now = datetime.now(tz=UTC)
    batch = [
        [
            r["organization_id"],
            r["crop_rotation_id"],
            r["field_id"],
            r["field_name"],
            r["season_id"],
            r["season_name"],
            r["contour_id"],
            r["contour_name"],
            r["culture"],
            r["cultivar"],
            r["start_date"],
            r["end_date"],
            float(r["contour_area_ha"]) if r["contour_area_ha"] is not None else None,
            (r["description"] or None),
            float(r["harvest_yield_t_per_ha"])
            if r.get("harvest_yield_t_per_ha") is not None
            else None,
            now,
        ]
        for r in rows
    ]
    ch.command("TRUNCATE TABLE IF EXISTS fact_crop_rotation")
    if batch:
        ch.insert(
            "fact_crop_rotation",
            batch,
            column_names=[
                "organization_id",
                "crop_rotation_id",
                "field_id",
                "field_name",
                "season_id",
                "season_name",
                "contour_id",
                "contour_name",
                "culture",
                "cultivar",
                "start_date",
                "end_date",
                "contour_area_ha",
                "description",
                "harvest_yield_t_per_ha",
                "synced_at",
            ],
        )
    log.info("fact_crop_rotation synced: %s rows", len(batch))


def _sync_dzz(ch, dzz_url: str, field_org: dict[str, str]) -> None:
    if not dzz_url or not field_org:
        return
    wm = _get_watermark(ch, "dzz")
    with _connect(dzz_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    field_id::text,
                    season_id::text,
                    contour_id,
                    scene_id,
                    scene_date,
                    collection_name,
                    sensor,
                    cloud_cover,
                    ndvi,
                    evi,
                    ndwi,
                    msavi,
                    updated_at
                FROM dzz_scene_analytics
                WHERE updated_at > %s
                ORDER BY updated_at ASC
                LIMIT 8000
                """,
                (wm,),
            )
            raw = cur.fetchall()
    if not raw:
        return
    batch: list[list[Any]] = []
    max_wm = wm
    for r in raw:
        org = field_org.get(r["field_id"])
        if not org:
            continue
        u = r["updated_at"]
        if isinstance(u, datetime) and u.tzinfo is None:
            u = u.replace(tzinfo=UTC)
        max_wm = max(max_wm, u)
        batch.append(
            [
                org,
                r["field_id"],
                r["season_id"],
                r["contour_id"] or "",
                r["scene_id"],
                r["scene_date"],
                r["collection_name"],
                r["sensor"],
                r["cloud_cover"],
                r["ndvi"],
                r["evi"],
                r["ndwi"],
                r["msavi"],
                u,
            ]
        )
    if batch:
        ch.insert(
            "fact_dzz_scene",
            batch,
            column_names=[
                "organization_id",
                "field_id",
                "season_id",
                "contour_id",
                "scene_id",
                "scene_date",
                "collection_name",
                "sensor",
                "cloud_cover",
                "ndvi",
                "evi",
                "ndwi",
                "msavi",
                "updated_at",
            ],
        )
        _set_watermark(ch, "dzz", max_wm)
        log.info("fact_dzz_scene +%s rows", len(batch))


def _sync_meteo(ch, meteo_url: str, field_org: dict[str, str]) -> None:
    if not meteo_url or not field_org:
        return
    wm = _get_watermark(ch, "meteo")
    with _connect(meteo_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    field_id::text,
                    date_time,
                    temperature,
                    humidity,
                    wind_speed,
                    precipitation,
                    soil_moisture_0_to_1cm
                FROM meteo_data
                WHERE date_time > %s
                ORDER BY date_time ASC
                LIMIT 8000
                """,
                (wm,),
            )
            raw = cur.fetchall()
    if not raw:
        return
    batch: list[list[Any]] = []
    max_wm = wm
    for r in raw:
        org = field_org.get(r["field_id"])
        if not org:
            continue
        dt = r["date_time"]
        if isinstance(dt, datetime) and dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        max_wm = max(max_wm, dt)
        batch.append(
            [
                org,
                r["field_id"],
                dt,
                r["temperature"],
                r["humidity"],
                r["wind_speed"],
                r["precipitation"],
                r["soil_moisture_0_to_1cm"],
            ]
        )
    if batch:
        ch.insert(
            "fact_meteo_observation",
            batch,
            column_names=[
                "organization_id",
                "field_id",
                "date_time",
                "temperature",
                "humidity",
                "wind_speed",
                "precipitation",
                "soil_moisture_0_1cm",
            ],
        )
        _set_watermark(ch, "meteo", max_wm)
        log.info("fact_meteo_observation +%s rows", len(batch))


def _field_org_map_from_ch(ch) -> dict[str, str]:
    """Соответствие field_id → organization_id из dim_field (для ДЗЗ/метео без полного опроса полей)."""
    rows = ch.query(
        """
        SELECT field_id, organization_id
        FROM dim_field
        FINAL
        """
    ).result_rows
    return {str(r[0]): str(r[1]) for r in rows if r[0] and r[1]}


def sync_fields_warehouse() -> dict[str, str]:
    """Полная пересборка справочника полей и севооборота в ClickHouse из PostgreSQL fields."""
    if not settings.FIELDS_DATABASE_URL:
        return {}
    ch = get_client()
    field_org = _sync_dim_fields(ch, settings.FIELDS_DATABASE_URL)
    _sync_crop_rotations(ch, settings.FIELDS_DATABASE_URL)
    return field_org


def run_sync_cycle(*, include_fields: bool | None = None) -> None:
    if not settings.FIELDS_DATABASE_URL:
        log.warning("FIELDS_DATABASE_URL empty; skip sync")
        return
    ch = get_client()
    if include_fields is None:
        include_fields = not settings.CDC_ENABLED
    if include_fields:
        field_org = sync_fields_warehouse()
    else:
        field_org = _field_org_map_from_ch(ch)
        if not field_org:
            log.warning("dim_field пуста — одноразовая синхронизация полей из PostgreSQL")
            field_org = sync_fields_warehouse()
    _sync_dzz(ch, settings.DZZ_DATABASE_URL, field_org)
    _sync_meteo(ch, settings.METEO_DATABASE_URL, field_org)
