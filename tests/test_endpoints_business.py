from datetime import date, datetime
from fastapi import HTTPException, status

from fastapi import FastAPI
from fastapi.testclient import TestClient

import src.routers.analytics as analytics_module
from src.auth_dep import TokenPayload


class _QueryResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeClickHouseClient:
    def query(self, sql, parameters):
        normalized_sql = " ".join(sql.split()).lower()

        if "from fact_dzz_scene" in normalized_sql and "avg(ndvi)" not in normalized_sql:
            return _QueryResult([(5, date(2026, 4, 22), datetime(2026, 4, 22, 12, 0, 0))])
        if "from fact_meteo_observation" in normalized_sql and "todate(date_time)" not in normalized_sql:
            return _QueryResult([(10, datetime(2026, 4, 22, 11, 0, 0))])
        if "from dim_field" in normalized_sql and "sum(contour_count)" in normalized_sql:
            return _QueryResult([(2, 4, 82.5)])
        if "from fact_crop_rotation" in normalized_sql and "uniqexact(culture)" in normalized_sql:
            return _QueryResult([(6, 3, 50.5)])
        if (
            "union all" in normalized_sql
            and "from dim_field" in normalized_sql
            and "from fact_crop_rotation" in normalized_sql
            and "group by season_id" in normalized_sql
        ):
            return _QueryResult([("season-a", "Season A", date(2026, 4, 1))])
        if "from fact_dzz_scene" in normalized_sql and "avg(ndvi)" in normalized_sql:
            return _QueryResult([(date(2026, 4, 20), 0.61, 3)])
        if "from fact_meteo_observation" in normalized_sql and "todate(date_time)" in normalized_sql:
            return _QueryResult([(date(2026, 4, 20), 21.4, 1.2, 65.0)])
        if "from dim_field" in normalized_sql and "select field_id, field_name, field_area_ha, contour_count" in normalized_sql:
            return _QueryResult([("field-1", "Поле 1", 44.2, 2)])
        if "from fact_crop_rotation" in normalized_sql and "group by culture" in normalized_sql:
            return _QueryResult([("Пшеница", 3, 20.0)])
        if "from fact_crop_rotation" in normalized_sql and "tostartofmonth(start_date)" in normalized_sql:
            return _QueryResult([(date(2026, 4, 1), "Пшеница", 2)])
        if "from fact_crop_rotation" in normalized_sql and "order by start_date desc" in normalized_sql:
            return _QueryResult(
                [("Поле 1", "Season A", "Контур 1", "Пшеница", "Сорт 1", date(2026, 3, 1), None, 10.0, 4.2)]
            )
        raise AssertionError(f"Unexpected SQL in fake client: {sql}")


def _build_client(monkeypatch) -> TestClient:
    monkeypatch.setattr(analytics_module, "get_client", lambda: FakeClickHouseClient())
    app = FastAPI()
    app.include_router(analytics_module.router, prefix="/api/analytics")
    app.dependency_overrides[analytics_module.require_user] = lambda: TokenPayload(
        sub="user-1",
        role="organization",
        email="owner@example.com",
        org="org-1",
    )
    return TestClient(app)


def test_analytics_all_endpoints_positive(monkeypatch) -> None:
    client = _build_client(monkeypatch)

    assert client.get("/api/analytics/summary").status_code == 200
    assert client.get("/api/analytics/crops/seasons").status_code == 200
    assert client.get("/api/analytics/dzz/ndvi-daily", params={"days": 30}).status_code == 200
    assert client.get("/api/analytics/meteo/daily", params={"days": 30}).status_code == 200
    assert client.get("/api/analytics/fields/top-area", params={"limit": 5}).status_code == 200
    assert client.get("/api/analytics/crops/by-culture").status_code == 200
    assert client.get("/api/analytics/crops/timeline-starts", params={"months": 12}).status_code == 200
    assert client.get("/api/analytics/crops/records", params={"limit": 50}).status_code == 200


def test_analytics_negative_query_validation(monkeypatch) -> None:
    client = _build_client(monkeypatch)

    # ge=7
    assert client.get("/api/analytics/dzz/ndvi-daily", params={"days": 1}).status_code == 422
    # le=365
    assert client.get("/api/analytics/meteo/daily", params={"days": 500}).status_code == 422
    # le=50
    assert client.get("/api/analytics/fields/top-area", params={"limit": 100}).status_code == 422
    # le=CROP_RECORDS_MAX_LIMIT
    assert client.get("/api/analytics/crops/records", params={"limit": 1000000}).status_code == 422


def test_analytics_summary_accepts_trimmed_season_id(monkeypatch) -> None:
    client = _build_client(monkeypatch)
    response = client.get("/api/analytics/summary", params={"season_id": "  season-a  "})
    assert response.status_code == 200
    assert response.json()["filter_season_id"] == "season-a"


def test_analytics_summary_season_inventory_from_dim_field(monkeypatch) -> None:
    """Для выбранного сезона поля/контуры/площадь берутся из dim_field (инвентарь),
    а не из fact_crop_rotation: контуры без севооборота тоже учитываются."""
    client = _build_client(monkeypatch)
    body = client.get("/api/analytics/summary", params={"season_id": "season-a"}).json()

    # инвентарь сезона из dim_field (2, 4, 82.5)
    assert body["fields"] == 2
    assert body["contours_total"] == 4
    assert body["area_ha_total"] == 82.5
    # севооборотные показатели из fact_crop_rotation (6, 3, 50.5)
    assert body["crop_rotation_records"] == 6
    assert body["crop_rotation_cultures_distinct"] == 3
    assert body["crop_rotation_area_ha_sum"] == 50.5


def test_analytics_unauthorized_when_dependency_fails(monkeypatch) -> None:
    monkeypatch.setattr(analytics_module, "get_client", lambda: FakeClickHouseClient())
    app = FastAPI()
    app.include_router(analytics_module.router, prefix="/api/analytics")
    app.dependency_overrides[analytics_module.require_user] = lambda: (_ for _ in ()).throw(
        HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")
    )
    client = TestClient(app)

    response = client.get("/api/analytics/summary")
    assert response.status_code == 401
