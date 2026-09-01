"""Unit tests for the API database query layer (connections mocked)."""

from datetime import datetime
from unittest.mock import MagicMock, patch

from api import db
from api.config import Settings

_SETTINGS = Settings()


def test_check_ready_reports_each_service(monkeypatch):
    monkeypatch.setattr(db, "_postgres_ok", lambda settings: True)
    monkeypatch.setattr(db, "_clickhouse_ok", lambda settings: False)
    monkeypatch.setattr(db, "_kafka_ok", lambda settings: True)
    assert db.check_ready(_SETTINGS) == {
        "postgres": True,
        "clickhouse": False,
        "kafka": True,
    }


def test_query_summary_maps_rows():
    cursor_row = MagicMock()
    cursor_row.fetchone.return_value = (10, 3, 2, datetime(2026, 8, 27, 15, 1))
    cursor_campaigns = MagicMock()
    cursor_campaigns.fetchone.return_value = (5,)
    conn = MagicMock()
    conn.execute.side_effect = [cursor_row, cursor_campaigns]

    with patch("api.db.psycopg.connect") as connect:
        connect.return_value.__enter__.return_value = conn
        result = db.query_summary(_SETTINGS)

    assert result == {
        "total_views": 10,
        "total_users": 3,
        "pages": 2,
        "campaigns": 5,
        "latest_window": "2026-08-27T15:01:00",
    }


def test_query_top_pages_maps_rows():
    cursor = MagicMock()
    cursor.fetchall.return_value = [("/home", 8, 2), ("/product/1", 5, 1)]
    conn = MagicMock()
    conn.execute.return_value = cursor

    with patch("api.db.psycopg.connect") as connect:
        connect.return_value.__enter__.return_value = conn
        result = db.query_top_pages(_SETTINGS, 10)

    assert result == [
        {"page": "/home", "views": 8, "users": 2},
        {"page": "/product/1", "views": 5, "users": 1},
    ]


def test_query_top_campaigns_maps_rows():
    cursor = MagicMock()
    cursor.fetchall.return_value = [("organic", 5, 4, 1, 0)]
    conn = MagicMock()
    conn.execute.return_value = cursor

    with patch("api.db.psycopg.connect") as connect:
        connect.return_value.__enter__.return_value = conn
        result = db.query_top_campaigns(_SETTINGS, 10)

    assert result == [
        {
            "campaign_id": "organic",
            "events": 5,
            "views": 4,
            "clicks": 1,
            "conversions": 0,
        }
    ]


def test_query_recent_events_maps_rows():
    client = MagicMock()
    client.query.return_value.result_rows = [
        (
            "e1",
            "page_view",
            "u-1",
            "/home",
            "mobile",
            "us-west",
            "organic",
            "google",
            datetime(2026, 8, 27, 15, 0),
        )
    ]

    with patch("api.db._clickhouse_client", return_value=client):
        rows = db.query_recent_events(_SETTINGS, 10)

    assert rows[0]["event_id"] == "e1"
    assert rows[0]["page"] == "/home"
    assert rows[0]["ts"] == datetime(2026, 8, 27, 15, 0)


def test_query_timeline_maps_rows():
    client = MagicMock()
    client.query.return_value.result_rows = [
        (datetime(2026, 8, 27, 15, 0), "/home", "mobile", 8, 2)
    ]

    with patch("api.db._clickhouse_client", return_value=client):
        rows = db.query_timeline(_SETTINGS, 10)

    assert rows[0] == {
        "window_start": datetime(2026, 8, 27, 15, 0),
        "page": "/home",
        "device": "mobile",
        "views": 8,
        "users": 2,
    }


def test_kafka_topics_maps_partitions():
    admin = MagicMock()
    admin.list_topics.return_value = {"clicks.raw": {0, 1, 2}, "clicks.dlq": {0, 1, 2}}

    with patch("api.db._kafka_admin", return_value=admin):
        result = db.kafka_topics(_SETTINGS)

    assert result == [
        {"topic": "clicks.dlq", "partitions": 3},
        {"topic": "clicks.raw", "partitions": 3},
    ]
