from datetime import date, datetime

from fastapi import FastAPI
from fastapi.testclient import TestClient

import src.routers.analytics as analytics_module
from src.auth_dep import TokenPayload


class _QueryResult:
    def __init__(self, row):
        self.result_rows = [row]


class FakeClickHouseClient:
    def __init__(self):
        self.calls = []

    def query(self, sql, parameters):
        self.calls.append((sql, parameters))

        if "FROM fact_dzz_scene" in sql:
            return _QueryResult((7, date(2026, 4, 10), datetime(2026, 4, 11, 12, 30, 0)))
        if "FROM fact_meteo_observation" in sql:
            return _QueryResult((19, datetime(2026, 4, 11, 9, 0, 0)))
        if "uniqExact(field_id)" in sql:
            return _QueryResult((3, 8, 150.75))
        if "FROM fact_crop_rotation" in sql and "uniqExact(culture)" in sql:
            return _QueryResult((12, 4, 120.4))

        raise AssertionError(f"Unexpected SQL in test fake client: {sql}")


def test_crop_where_without_season_uses_only_org() -> None:
    where_sql, params = analytics_module._crop_where("org-1", None)

    assert where_sql == "organization_id = {org:String}"
    assert params == {"org": "org-1"}


def test_crop_where_with_season_trims_value() -> None:
    where_sql, params = analytics_module._crop_where("org-1", "  season-42  ")

    assert where_sql == "organization_id = {org:String} AND season_id = {season_id:String}"
    assert params == {"org": "org-1", "season_id": "season-42"}


def test_summary_endpoint_applies_trimmed_season_filter(monkeypatch) -> None:
    fake_ch = FakeClickHouseClient()
    monkeypatch.setattr(analytics_module, "get_client", lambda: fake_ch)

    app = FastAPI()
    app.include_router(analytics_module.router, prefix="/api/analytics")
    app.dependency_overrides[analytics_module.require_user] = lambda: TokenPayload(
        sub="user-1",
        role="organization",
        email="owner@example.com",
        org="org-1",
    )

    client = TestClient(app)
    response = client.get("/api/analytics/summary", params={"season_id": "  season-42  "})

    assert response.status_code == 200
    data = response.json()
    assert data["filter_season_id"] == "season-42"
    assert data["fields"] == 3
    assert data["contours_total"] == 8
    assert data["area_ha_total"] == 150.75
    assert data["dzz_scenes"] == 7

    crop_calls = [
        params
        for sql, params in fake_ch.calls
        if "FROM fact_crop_rotation" in sql and "uniqExact(culture)" in sql
    ]
    assert crop_calls
    assert crop_calls[0]["season_id"] == "season-42"
