from typing import Optional

from fastapi import APIRouter, Depends, Query

from src.auth_dep import TokenPayload, require_user
from src.clickhouse_client import get_client

router = APIRouter(tags=["analytics"])

# Upper bound for dashboard records (Gantt can render up to this limit safely).
CROP_RECORDS_MAX_LIMIT = 10_000


def _crop_where(org: str, season_id: Optional[str]) -> tuple[str, dict]:
    """Build WHERE clause for fact_crop_rotation by organization and optional season."""
    if season_id and season_id.strip():
        sid = season_id.strip()
        return (
            "organization_id = {org:String} AND season_id = {season_id:String}",
            {"org": org, "season_id": sid},
        )
    return "organization_id = {org:String}", {"org": org}


@router.get("/summary")
def analytics_summary(
    season_id: Optional[str] = Query(
        default=None,
        description="UUID сезона; без параметра — по всей организации",
    ),
    user: TokenPayload = Depends(require_user),
):
    ch = get_client()
    org = user.org
    crop_sql, crop_params = _crop_where(org, season_id)
    dzz = ch.query(
        """
        SELECT count(), max(scene_date), max(updated_at)
        FROM fact_dzz_scene
        WHERE organization_id = {org:String}
        """,
        parameters={"org": org},
    ).result_rows[0]
    meteo = ch.query(
        """
        SELECT count(), max(date_time)
        FROM fact_meteo_observation
        WHERE organization_id = {org:String}
        """,
        parameters={"org": org},
    ).result_rows[0]
    if season_id and season_id.strip():
        fields = ch.query(
            f"""
            SELECT
                uniqExact(field_id),
                uniqExact(contour_id),
                sum(max_ha)
            FROM (
                SELECT
                    field_id,
                    contour_id,
                    max(coalesce(contour_area_ha, 0)) AS max_ha
                FROM fact_crop_rotation
                WHERE {crop_sql}
                GROUP BY field_id, contour_id
            ) AS by_field_contour
            """,
            parameters=crop_params,
        ).result_rows[0]
    else:
        fields = ch.query(
            """
            SELECT count(), sum(contour_count), sum(coalesce(field_area_ha, 0))
            FROM dim_field
            WHERE organization_id = {org:String}
            """,
            parameters={"org": org},
        ).result_rows[0]
    crops = ch.query(
        f"""
        SELECT
            count(),
            uniqExact(culture),
            sum(coalesce(contour_area_ha, 0))
        FROM fact_crop_rotation
        WHERE {crop_sql}
        """,
        parameters=crop_params,
    ).result_rows[0]
    dzz_n = int(dzz[0] or 0)
    meteo_n = int(meteo[0] or 0)
    crop_n = int(crops[0] or 0)
    return {
        "organization_id": org,
        "fields": int(fields[0] or 0),
        "contours_total": int(fields[1] or 0),
        "area_ha_total": float(fields[2] or 0),
        "dzz_scenes": dzz_n,
        "dzz_last_scene_date": dzz[1].isoformat() if dzz_n and dzz[1] else None,
        "dzz_last_updated": dzz[2].isoformat() if dzz_n and dzz[2] else None,
        "meteo_observations": meteo_n,
        "meteo_last_at": meteo[1].isoformat() if meteo_n and meteo[1] else None,
        "crop_rotation_records": crop_n,
        "crop_rotation_cultures_distinct": int(crops[1] or 0),
        "crop_rotation_area_ha_sum": float(crops[2] or 0),
        "filter_season_id": season_id.strip() if season_id and season_id.strip() else None,
    }


@router.get("/crops/seasons")
def crops_seasons(user: TokenPayload = Depends(require_user)):
    ch = get_client()
    rows = ch.query(
        """
        SELECT
            season_id,
            any(season_name) AS season_name,
            max(start_date) AS last_start
        FROM fact_crop_rotation
        WHERE organization_id = {org:String}
        GROUP BY season_id
        ORDER BY last_start DESC, season_name
        """,
        parameters={"org": user.org},
    ).result_rows
    return {
        "items": [
            {
                "season_id": r[0],
                "season_name": r[1],
            }
            for r in rows
        ]
    }


@router.get("/dzz/ndvi-daily")
def dzz_ndvi_daily(
    days: int = Query(default=90, ge=7, le=730),
    user: TokenPayload = Depends(require_user),
):
    ch = get_client()
    rows = ch.query(
        """
        SELECT
            toDate(scene_date) AS d,
            avg(ndvi) AS avg_ndvi,
            count() AS n
        FROM fact_dzz_scene
        WHERE organization_id = {org:String}
          AND scene_date >= today() - {days:UInt32}
          AND ndvi IS NOT NULL
        GROUP BY d
        ORDER BY d
        """,
        parameters={"org": user.org, "days": days},
    ).result_rows
    return {
        "series": [
            {"date": str(r[0]), "avg_ndvi": float(r[1]), "scenes": int(r[2])} for r in rows
        ]
    }


@router.get("/meteo/daily")
def meteo_daily(
    days: int = Query(default=30, ge=7, le=365),
    user: TokenPayload = Depends(require_user),
):
    ch = get_client()
    rows = ch.query(
        """
        SELECT
            toDate(date_time) AS d,
            avg(temperature) AS t,
            sum(coalesce(precipitation, 0)) AS p,
            avg(humidity) AS h
        FROM fact_meteo_observation
        WHERE organization_id = {org:String}
          AND date_time >= now() - toIntervalDay({days:UInt32})
        GROUP BY d
        ORDER BY d
        """,
        parameters={"org": user.org, "days": days},
    ).result_rows
    return {
        "series": [
            {
                "date": str(r[0]),
                "avg_temperature": float(r[1]) if r[1] is not None else None,
                "sum_precipitation": float(r[2]) if r[2] is not None else None,
                "avg_humidity": float(r[3]) if r[3] is not None else None,
            }
            for r in rows
        ]
    }


@router.get("/fields/top-area")
def fields_top_area(
    limit: int = Query(default=8, ge=1, le=50),
    user: TokenPayload = Depends(require_user),
):
    ch = get_client()
    rows = ch.query(
        """
        SELECT field_id, field_name, field_area_ha, contour_count
        FROM dim_field
        WHERE organization_id = {org:String}
          AND field_area_ha IS NOT NULL
        ORDER BY field_area_ha DESC
        LIMIT {lim:UInt32}
        """,
        parameters={"org": user.org, "lim": limit},
    ).result_rows
    return {
        "items": [
            {
                "field_id": r[0],
                "field_name": r[1],
                "area_ha": float(r[2]) if r[2] is not None else None,
                "contour_count": int(r[3] or 0),
            }
            for r in rows
        ]
    }


@router.get("/crops/by-culture")
def crops_by_culture(
    season_id: Optional[str] = Query(default=None),
    user: TokenPayload = Depends(require_user),
):
    ch = get_client()
    w, params = _crop_where(user.org, season_id)
    rows = ch.query(
        f"""
        SELECT
            culture,
            count() AS records,
            sum(coalesce(contour_area_ha, 0)) AS area_ha
        FROM fact_crop_rotation
        WHERE {w}
        GROUP BY culture
        ORDER BY records DESC, culture
        """,
        parameters=params,
    ).result_rows
    return {
        "items": [
            {
                "culture": r[0],
                "records": int(r[1] or 0),
                "area_ha": float(r[2]) if r[2] is not None else 0.0,
            }
            for r in rows
        ]
    }


@router.get("/crops/timeline-starts")
def crops_timeline_starts(
    months: int = Query(default=36, ge=3, le=120),
    season_id: Optional[str] = Query(default=None),
    user: TokenPayload = Depends(require_user),
):
    ch = get_client()
    w, params = _crop_where(user.org, season_id)
    params = {**params, "months": months}
    rows = ch.query(
        f"""
        SELECT
            toStartOfMonth(start_date) AS month,
            culture,
            count() AS n
        FROM fact_crop_rotation
        WHERE {w}
          AND start_date >= today() - toIntervalMonth({{months:UInt32}})
        GROUP BY month, culture
        ORDER BY month, culture
        """,
        parameters=params,
    ).result_rows
    return {
        "series": [
            {
                "month": str(r[0])[:7],
                "culture": r[1],
                "count": int(r[2] or 0),
            }
            for r in rows
        ]
    }


@router.get("/crops/records")
def crops_records(
    limit: int = Query(default=500, ge=1, le=CROP_RECORDS_MAX_LIMIT),
    season_id: Optional[str] = Query(default=None),
    user: TokenPayload = Depends(require_user),
):
    ch = get_client()
    w, params = _crop_where(user.org, season_id)
    params = {**params, "lim": limit}
    rows = ch.query(
        f"""
        SELECT
            field_name,
            season_name,
            contour_name,
            culture,
            cultivar,
            start_date,
            end_date,
            contour_area_ha,
            harvest_yield_t_per_ha
        FROM fact_crop_rotation
        WHERE {w}
        ORDER BY start_date DESC, field_name, culture
        LIMIT {{lim:UInt32}}
        """,
        parameters=params,
    ).result_rows
    return {
        "items": [
            {
                "field_name": r[0],
                "season_name": r[1],
                "contour_name": r[2],
                "culture": r[3],
                "cultivar": r[4],
                "start_date": r[5].isoformat() if r[5] else None,
                "end_date": r[6].isoformat() if r[6] else None,
                "contour_area_ha": float(r[7]) if r[7] is not None else None,
                "harvest_yield_t_per_ha": float(r[8]) if r[8] is not None else None,
            }
            for r in rows
        ]
    }
