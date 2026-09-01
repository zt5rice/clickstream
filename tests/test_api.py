"""Unit tests for API endpoints (database access mocked)."""

import pytest
from fastapi.testclient import TestClient

from api import db, main


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setattr(
        db,
        "check_ready",
        lambda settings: {"postgres": True, "clickhouse": True, "kafka": True},
    )
    monkeypatch.setattr(
        db,
        "query_summary",
        lambda settings: {
            "total_views": 12,
            "total_users": 3,
            "pages": 2,
            "campaigns": 2,
            "latest_window": "2026-08-27T15:01:00Z",
        },
    )
    monkeypatch.setattr(
        db,
        "query_top_pages",
        lambda settings, limit: [{"page": "/home", "views": 8, "users": 2}],
    )
    monkeypatch.setattr(
        db,
        "query_top_campaigns",
        lambda settings, limit: [
            {"campaign_id": "organic", "events": 5, "views": 4, "clicks": 1, "conversions": 0}
        ],
    )
    monkeypatch.setattr(
        db,
        "query_recent_events",
        lambda settings, limit: [{"event_id": "e1", "event_type": "page_view"}],
    )
    monkeypatch.setattr(
        db,
        "query_timeline",
        lambda settings, limit: [
            {
                "window_start": "2026-08-27T15:00:00Z",
                "page": "/home",
                "device": "mobile",
                "views": 8,
                "users": 2,
            }
        ],
    )
    monkeypatch.setattr(
        db,
        "kafka_topics",
        lambda settings: [{"topic": "clicks.raw", "partitions": 3}],
    )
    return TestClient(main.app)


def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_ready_ok(client):
    resp = client.get("/ready")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ready"


def test_ready_503_when_unhealthy(monkeypatch):
    monkeypatch.setattr(
        db,
        "check_ready",
        lambda settings: {"postgres": False, "clickhouse": False, "kafka": False},
    )
    resp = TestClient(main.app).get("/ready")
    assert resp.status_code == 503
    assert resp.json()["detail"]["kafka"] is False


def test_summary(client):
    resp = client.get("/api/v1/summary")
    assert resp.status_code == 200
    assert resp.json()["total_views"] == 12


def test_top_pages(client):
    resp = client.get("/api/v1/top/pages?limit=5")
    assert resp.status_code == 200
    assert resp.json()["pages"][0]["page"] == "/home"


def test_top_campaigns(client):
    resp = client.get("/api/v1/top/campaigns")
    assert resp.status_code == 200
    assert resp.json()["campaigns"][0]["campaign_id"] == "organic"


def test_recent_events(client):
    resp = client.get("/api/v1/events/recent?limit=10")
    assert resp.status_code == 200
    assert resp.json()["events"][0]["event_id"] == "e1"


def test_timeline(client):
    resp = client.get("/api/v1/timeline")
    assert resp.status_code == 200
    assert resp.json()["windows"][0]["page"] == "/home"


def test_topics(client):
    resp = client.get("/api/v1/health/topics")
    assert resp.status_code == 200
    assert resp.json()["topics"][0]["partitions"] == 3
