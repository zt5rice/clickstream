-- ClickHouse init: olap database + tables.
-- Keep in sync with spark_jobs/clickhouse_sink.py (SCHEMA_DDL).

CREATE DATABASE IF NOT EXISTS olap;

CREATE TABLE IF NOT EXISTS olap.clicks (
    event_id String,
    event_type String,
    user_id String,
    session_id String,
    ts DateTime,
    page String,
    device String,
    region String,
    campaign_id String,
    referrer String
) ENGINE = ReplacingMergeTree()
ORDER BY event_id;

CREATE TABLE IF NOT EXISTS olap.page_views_1m (
    window_start DateTime,
    page String,
    device String,
    views UInt64,
    users UInt64
) ENGINE = ReplacingMergeTree()
ORDER BY (window_start, page, device);
