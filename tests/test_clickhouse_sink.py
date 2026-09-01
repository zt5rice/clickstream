"""Unit tests for the ClickHouse sink (no ClickHouse server required)."""

from spark_jobs.clickhouse_sink import (
    CLICKS_COLUMNS,
    PAGE_VIEWS_COLUMNS,
    SCHEMA_DDL,
    client_params,
)


def test_schema_ddl_uses_replacing_merge_tree():
    ddl = "\n".join(SCHEMA_DDL)
    assert "CREATE DATABASE IF NOT EXISTS olap" in ddl
    assert "CREATE TABLE IF NOT EXISTS olap.clicks" in ddl
    assert "CREATE TABLE IF NOT EXISTS olap.page_views_1m" in ddl
    assert ddl.count("ENGINE = ReplacingMergeTree()") == 2
    assert "ORDER BY event_id" in ddl
    assert "ORDER BY (window_start, page, device)" in ddl


def test_clicks_columns_match_event_schema():
    assert CLICKS_COLUMNS == (
        "event_id",
        "event_type",
        "user_id",
        "session_id",
        "ts",
        "page",
        "device",
        "region",
        "campaign_id",
        "referrer",
    )


def test_page_views_columns():
    assert PAGE_VIEWS_COLUMNS == ("window_start", "page", "device", "views", "users")


def test_client_params_reads_env(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_HOST", "ch.example")
    monkeypatch.setenv("CLICKHOUSE_PORT", "8124")
    monkeypatch.setenv("CLICKHOUSE_USER", "reader")
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "pw")
    monkeypatch.setenv("CLICKHOUSE_DATABASE", "analytics")
    params = client_params()
    assert params["host"] == "ch.example"
    assert params["port"] == 8124
    assert params["username"] == "reader"
    assert params["password"] == "pw"
    assert params["database"] == "analytics"
