"""Unit tests for the P2-08 AI assistant (SQL allowlist, anomaly, mock LLM)."""

from __future__ import annotations

import pytest
from ai_assistant.anomaly import detect_anomalies
from ai_assistant.llm import MockLLM
from ai_assistant.sql import ALLOWED_TABLES, build_select_sql, sanitize_sql
from fastapi.testclient import TestClient


def test_build_select_sql_allowlisted_table() -> None:
    sql = build_select_sql("curated.daily_page_summary", columns=["day", "views"], limit=10)
    assert sql == "SELECT day, views FROM curated.daily_page_summary LIMIT 10"


def test_build_select_sql_rejects_unknown_table() -> None:
    with pytest.raises(ValueError):
        build_select_sql("curated.secrets")


def test_sanitize_sql_rejects_write() -> None:
    with pytest.raises(ValueError):
        sanitize_sql("DELETE FROM curated.daily_page_summary")


def test_sanitize_sql_rejects_unknown_table() -> None:
    with pytest.raises(ValueError):
        sanitize_sql("SELECT * FROM curated.secrets")


def test_sanitize_sql_accepts_plain_select() -> None:
    sql = sanitize_sql("select * from curated.daily_page_summary limit 5")
    assert sql.lower() == "select * from curated.daily_page_summary limit 5"


def test_detect_anomalies_finds_outlier() -> None:
    values = [10.0] * 100 + [500.0]
    found = detect_anomalies(values, threshold=3.0)
    assert len(found) == 1
    assert found[0].index == 100
    assert found[0].value == 500.0


def test_detect_anomalies_ignores_flat_series() -> None:
    assert detect_anomalies([1.0, 1.0, 1.0, 1.0], threshold=3.0) == []


def test_mock_llm_generates_allowlisted_sql() -> None:
    sql = MockLLM().generate_sql("curated.daily_page_summary", "daily views")
    assert sql.startswith("SELECT")
    assert "curated.daily_page_summary" in sql


def test_mock_llm_explains_alert() -> None:
    text = MockLLM().explain_alert("APIP95LatencyHigh", "1.2", "p95 above 1s")
    assert "APIP95LatencyHigh" in text


def test_api_generate_sql_endpoint() -> None:
    from ai_assistant.main import app

    client = TestClient(app)
    resp = client.post(
        "/assistant/generate-sql",
        json={"table": "curated.daily_page_summary", "question": "views by day"},
    )
    assert resp.status_code == 200
    assert resp.json()["sql"].startswith("SELECT")
    assert resp.json()["table"] == "curated.daily_page_summary"


def test_allowed_tables_nonempty() -> None:
    assert len(ALLOWED_TABLES) >= 4
