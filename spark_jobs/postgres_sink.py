"""PostgreSQL sink for curated aggregations (idempotent ``ON CONFLICT`` upserts).

Each micro-batch is written through a staging table, then merged into the
curated table with ``INSERT ... ON CONFLICT (...) DO UPDATE``. Spark windowed
aggregations emit the *cumulative* state of each window, so replacing the
existing row per batch is correct and idempotent (safe to replay).

Writing/upserting via psycopg keeps the Spark image free of a JDBC driver.
"""

from __future__ import annotations

import os
from collections.abc import Sequence
from typing import TYPE_CHECKING

import psycopg

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

SCHEMA = "curated"
PAGE_VIEWS_TABLE = "page_views_1m"
CAMPAIGN_STATS_TABLE = "campaign_stats_1m"

PAGE_VIEWS_KEY = ("window_start", "page", "device")
CAMPAIGN_STATS_KEY = ("window_start", "campaign_id")

PAGE_VIEWS_COLUMNS = ("window_start", "page", "device", "views", "users")
CAMPAIGN_STATS_COLUMNS = (
    "window_start",
    "campaign_id",
    "events",
    "views",
    "clicks",
    "conversions",
    "users",
)

SCHEMA_DDL = (
    "CREATE SCHEMA IF NOT EXISTS curated",
    """
    CREATE TABLE IF NOT EXISTS curated.page_views_1m (
        window_start TIMESTAMP NOT NULL,
        page TEXT NOT NULL,
        device TEXT NOT NULL,
        views BIGINT NOT NULL DEFAULT 0,
        users BIGINT NOT NULL DEFAULT 0,
        PRIMARY KEY (window_start, page, device)
    )
    """,
    "CREATE TABLE IF NOT EXISTS curated.page_views_1m_staging "
    "(LIKE curated.page_views_1m INCLUDING DEFAULTS)",
    """
    CREATE TABLE IF NOT EXISTS curated.campaign_stats_1m (
        window_start TIMESTAMP NOT NULL,
        campaign_id TEXT NOT NULL,
        events BIGINT NOT NULL DEFAULT 0,
        views BIGINT NOT NULL DEFAULT 0,
        clicks BIGINT NOT NULL DEFAULT 0,
        conversions BIGINT NOT NULL DEFAULT 0,
        users BIGINT NOT NULL DEFAULT 0,
        PRIMARY KEY (window_start, campaign_id)
    )
    """,
    "CREATE TABLE IF NOT EXISTS curated.campaign_stats_1m_staging "
    "(LIKE curated.campaign_stats_1m INCLUDING DEFAULTS)",
)


def dsn() -> str:
    """Build a libpq DSN from environment variables."""
    return (
        f"host={os.environ.get('POSTGRES_HOST', 'localhost')} "
        f"port={os.environ.get('POSTGRES_PORT', '5432')} "
        f"user={os.environ.get('POSTGRES_USER', 'click')} "
        f"password={os.environ.get('POSTGRES_PASSWORD', 'click')} "
        f"dbname={os.environ.get('POSTGRES_DB', 'clickstream')}"
    )


def insert_sql(table: str, columns: Sequence[str]) -> str:
    if not columns:
        raise ValueError("columns must not be empty")
    col_sql = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    return f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders})"


def upsert_sql(
    staging_table: str,
    table: str,
    columns: Sequence[str],
    key_columns: Sequence[str],
) -> str:
    if not columns:
        raise ValueError("columns must not be empty")
    if not key_columns:
        raise ValueError("key_columns must not be empty")
    col_sql = ", ".join(columns)
    key_sql = ", ".join(key_columns)
    update_columns = [column for column in columns if column not in key_columns]
    update_sql = ", ".join(f"{column} = EXCLUDED.{column}" for column in update_columns)
    return (
        f"INSERT INTO {table} ({col_sql})\n"
        f"SELECT {col_sql} FROM {staging_table}\n"
        f"ON CONFLICT ({key_sql}) DO UPDATE SET {update_sql}"
    )


def ensure_schema(conn: psycopg.Connection) -> None:
    """Create the curated schema, tables, and staging tables if missing."""
    with conn.cursor() as cur:
        for statement in SCHEMA_DDL:
            cur.execute(statement)
    conn.commit()


def upsert_batch(
    df: DataFrame,
    table: str,
    key_columns: Sequence[str],
    columns: Sequence[str],
) -> None:
    """Upsert one micro-batch into ``table`` via its staging table."""
    staging = f"{table}_staging"
    with psycopg.connect(dsn()) as conn:
        conn.execute(f"TRUNCATE {staging}")
        rows = [tuple(row) for row in df.select(*columns).collect()]
        if rows:
            with conn.cursor() as cur:
                cur.executemany(insert_sql(staging, columns), rows)
        conn.execute(upsert_sql(staging, table, columns, key_columns))
        conn.execute(f"TRUNCATE {staging}")
        conn.commit()


def upsert_page_views(batch_df: DataFrame, epoch_id: int) -> None:
    """foreachBatch callback for the page_views_1m aggregation."""
    upsert_batch(
        batch_df,
        f"{SCHEMA}.{PAGE_VIEWS_TABLE}",
        PAGE_VIEWS_KEY,
        PAGE_VIEWS_COLUMNS,
    )


def upsert_campaign_stats(batch_df: DataFrame, epoch_id: int) -> None:
    """foreachBatch callback for the campaign_stats_1m aggregation."""
    upsert_batch(
        batch_df,
        f"{SCHEMA}.{CAMPAIGN_STATS_TABLE}",
        CAMPAIGN_STATS_KEY,
        CAMPAIGN_STATS_COLUMNS,
    )
