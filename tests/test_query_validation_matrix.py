from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

import src.routers.analytics as analytics_module
from src.auth_dep import TokenPayload
from tests.test_endpoints_business import FakeClickHouseClient


def _client(monkeypatch) -> TestClient:
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


@pytest.mark.parametrize(
    ("path", "params", "expected_status"),
    [
        ("/api/analytics/summary", {}, 200),
        ("/api/analytics/summary", {"season_id": "  season-a  "}, 200),
        ("/api/analytics/dzz/ndvi-daily", {}, 200),
        ("/api/analytics/dzz/ndvi-daily", {"days": 6}, 422),
        ("/api/analytics/dzz/ndvi-daily", {"days": 7}, 200),
        ("/api/analytics/dzz/ndvi-daily", {"days": 731}, 422),
        ("/api/analytics/meteo/daily", {}, 200),
        ("/api/analytics/meteo/daily", {"days": 6}, 422),
        ("/api/analytics/meteo/daily", {"days": 365}, 200),
        ("/api/analytics/meteo/daily", {"days": 366}, 422),
        ("/api/analytics/fields/top-area", {}, 200),
        ("/api/analytics/fields/top-area", {"limit": 0}, 422),
        ("/api/analytics/fields/top-area", {"limit": 50}, 200),
        ("/api/analytics/fields/top-area", {"limit": 51}, 422),
        ("/api/analytics/crops/by-culture", {}, 200),
        ("/api/analytics/crops/timeline-starts", {}, 200),
        ("/api/analytics/crops/timeline-starts", {"months": 2}, 422),
        ("/api/analytics/crops/timeline-starts", {"months": 120}, 200),
        ("/api/analytics/crops/timeline-starts", {"months": 121}, 422),
        ("/api/analytics/crops/records", {"limit": 1000000}, 422),
    ],
)
def test_analytics_query_validation_matrix(path: str, params: dict, expected_status: int, monkeypatch) -> None:
    client = _client(monkeypatch)
    response = client.get(path, params=params)
    assert response.status_code == expected_status
