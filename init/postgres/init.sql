-- PostgreSQL init: curated schema + tables + indexes.
-- Keep in sync with spark_jobs/postgres_sink.py (SCHEMA_DDL).

CREATE SCHEMA IF NOT EXISTS curated;

CREATE TABLE IF NOT EXISTS curated.page_views_1m (
    window_start TIMESTAMP NOT NULL,
    page TEXT NOT NULL,
    device TEXT NOT NULL,
    views BIGINT NOT NULL DEFAULT 0,
    users BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (window_start, page, device)
);

CREATE TABLE IF NOT EXISTS curated.campaign_stats_1m (
    window_start TIMESTAMP NOT NULL,
    campaign_id TEXT NOT NULL,
    events BIGINT NOT NULL DEFAULT 0,
    views BIGINT NOT NULL DEFAULT 0,
    clicks BIGINT NOT NULL DEFAULT 0,
    conversions BIGINT NOT NULL DEFAULT 0,
    users BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (window_start, campaign_id)
);

CREATE INDEX IF NOT EXISTS idx_page_views_1m_window_start
    ON curated.page_views_1m (window_start);
CREATE INDEX IF NOT EXISTS idx_campaign_stats_1m_window_start
    ON curated.campaign_stats_1m (window_start);
