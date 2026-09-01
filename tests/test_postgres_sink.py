"""Unit tests for the PostgreSQL sink SQL generation (no Postgres required)."""

import pytest

from spark_jobs.postgres_sink import (
    CAMPAIGN_STATS_COLUMNS,
    CAMPAIGN_STATS_KEY,
    PAGE_VIEWS_COLUMNS,
    PAGE_VIEWS_KEY,
    SCHEMA_DDL,
    insert_sql,
    upsert_sql,
)


def test_insert_sql_builds_placeholders():
    sql = insert_sql("curated.page_views_1m_staging", PAGE_VIEWS_COLUMNS)
    assert sql == (
        "INSERT INTO curated.page_views_1m_staging "
        "(window_start, page, device, views, users) VALUES (%s, %s, %s, %s, %s)"
    )


def test_page_views_upsert_sql():
    sql = upsert_sql(
        "curated.page_views_1m_staging",
        "curated.page_views_1m",
        PAGE_VIEWS_COLUMNS,
        PAGE_VIEWS_KEY,
    )
    assert "ON CONFLICT (window_start, page, device)" in sql
    assert "views = EXCLUDED.views" in sql
    assert "users = EXCLUDED.users" in sql
    assert "window_start = EXCLUDED.window_start" not in sql
    assert (
        "SELECT window_start, page, device, views, users "
        "FROM curated.page_views_1m_staging"
    ) in sql


def test_campaign_stats_upsert_sql():
    sql = upsert_sql(
        "curated.campaign_stats_1m_staging",
        "curated.campaign_stats_1m",
        CAMPAIGN_STATS_COLUMNS,
        CAMPAIGN_STATS_KEY,
    )
    assert "ON CONFLICT (window_start, campaign_id)" in sql
    assert "events = EXCLUDED.events" in sql
    assert "clicks = EXCLUDED.clicks" in sql
    assert "conversions = EXCLUDED.conversions" in sql
    assert "campaign_id = EXCLUDED.campaign_id" not in sql


def test_schema_ddl_creates_tables_and_staging():
    ddl = "\n".join(SCHEMA_DDL)
    assert "CREATE SCHEMA IF NOT EXISTS curated" in ddl
    assert "CREATE TABLE IF NOT EXISTS curated.page_views_1m" in ddl
    assert "CREATE TABLE IF NOT EXISTS curated.page_views_1m_staging" in ddl
    assert "CREATE TABLE IF NOT EXISTS curated.campaign_stats_1m" in ddl
    assert "CREATE TABLE IF NOT EXISTS curated.campaign_stats_1m_staging" in ddl
    assert "PRIMARY KEY (window_start, page, device)" in ddl
    assert "PRIMARY KEY (window_start, campaign_id)" in ddl


def test_dsn_reads_env(monkeypatch):
    monkeypatch.setenv("POSTGRES_HOST", "pg.example")
    monkeypatch.setenv("POSTGRES_PORT", "6543")
    monkeypatch.setenv("POSTGRES_USER", "alice")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
    monkeypatch.setenv("POSTGRES_DB", "warehouse")

    from spark_jobs.postgres_sink import dsn

    assert dsn() == (
        "host=pg.example port=6543 user=alice "
        "password=secret dbname=warehouse"
    )


def test_upsert_batch_rejects_empty_columns():
    with pytest.raises(ValueError):
        upsert_sql("staging", "table", [], ["key"])
