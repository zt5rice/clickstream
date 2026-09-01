"""ClickHouse sink for raw events and window aggregates (clickhouse-connect HTTP).

``ReplacingMergeTree`` gives idempotent dedup on replay:

* ``olap.clicks``: ``ORDER BY event_id`` (re-inserting an event is a no-op)
* ``olap.page_views_1m``: ``ORDER BY (window_start, page, device)``

Uses ClickHouse's official HTTP client instead of a JDBC driver, so the Spark
image needs no extra jar (consistent with the API layer's stack).
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import clickhouse_connect

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

CLICKS_COLUMNS = (
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
PAGE_VIEWS_COLUMNS = ("window_start", "page", "device", "views", "users")

SCHEMA_DDL = (
    "CREATE DATABASE IF NOT EXISTS olap",
    """
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
    ORDER BY event_id
    """,
    """
    CREATE TABLE IF NOT EXISTS olap.page_views_1m (
        window_start DateTime,
        page String,
        device String,
        views UInt64,
        users UInt64
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (window_start, page, device)
    """,
)


def client_params() -> dict[str, object]:
    """Connection parameters for clickhouse-connect, read from the environment."""
    return {
        "host": os.environ.get("CLICKHOUSE_HOST", "localhost"),
        "port": int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        "username": os.environ.get("CLICKHOUSE_USER", "default"),
        "password": os.environ.get("CLICKHOUSE_PASSWORD", ""),
        "database": os.environ.get("CLICKHOUSE_DATABASE", "olap"),
    }


def get_client() -> clickhouse_connect.driver.Client:
    return clickhouse_connect.get_client(**client_params())


def ensure_schema(client: clickhouse_connect.driver.Client) -> None:
    """Create the olap database and tables if missing."""
    for statement in SCHEMA_DDL:
        client.command(statement)


def insert_clicks(batch_df: DataFrame, epoch_id: int) -> None:
    """foreachBatch callback writing raw events to ``olap.clicks``."""
    rows = [tuple(row) for row in batch_df.select(*CLICKS_COLUMNS).collect()]
    if not rows:
        return
    client = get_client()
    try:
        client.insert("olap.clicks", rows, column_names=list(CLICKS_COLUMNS))
    finally:
        client.close()


def insert_page_views(batch_df: DataFrame, epoch_id: int) -> None:
    """foreachBatch callback writing window aggregates to ``olap.page_views_1m``."""
    rows = [tuple(row) for row in batch_df.select(*PAGE_VIEWS_COLUMNS).collect()]
    if not rows:
        return
    client = get_client()
    try:
        client.insert("olap.page_views_1m", rows, column_names=list(PAGE_VIEWS_COLUMNS))
    finally:
        client.close()
