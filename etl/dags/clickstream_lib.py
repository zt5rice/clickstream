"""Shared helpers for the clickstream Airflow DAGs (P2-01).

Database helpers use psycopg (installed in the ``etl`` Docker image) and read
settings from environment variables set by docker-compose. The freshness math
is pure Python so it can be unit-tested without Airflow or Postgres.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.environ.get("POSTGRES_USER", "click")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "click")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "clickstream")

CURATED_TABLE = os.environ.get("CURATED_TABLE", "curated.page_views_1m")
FRESHNESS_WINDOW_SECONDS = int(os.environ.get("FRESHNESS_WINDOW_SECONDS", "60"))
FRESHNESS_MAX_STALE_SECONDS = int(os.environ.get("FRESHNESS_MAX_STALE_SECONDS", "180"))


def connect() -> psycopg.Connection[Any]:
    """Open a psycopg connection to the curated Postgres database."""
    return psycopg.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB,
        row_factory=dict_row,
    )


def latest_window_sql(table: str = CURATED_TABLE, column: str = "window_start") -> str:
    """Return SQL selecting the latest curated window start (NULL when empty)."""
    return f"SELECT MAX({column}) AS latest FROM {table}"


def fetch_latest_window(
    conn: psycopg.Connection[Any], table: str = CURATED_TABLE
) -> datetime | None:
    """Return the latest curated window start, or None for an empty table."""
    with conn.cursor() as cur:
        cur.execute(latest_window_sql(table))
        row = cur.fetchone()
    return row["latest"] if row else None


def stale_seconds(
    latest_window_start: datetime | None,
    now: datetime | None = None,
    window_seconds: int = FRESHNESS_WINDOW_SECONDS,
) -> float:
    """How far behind real time the curated layer is, in seconds.

    A window whose start is within ``window_seconds`` of ``now`` counts as
    current (freshness 0); an older window contributes its age minus one
    window. ``None`` (empty table) means infinitely stale.
    """
    now = now or datetime.now(UTC)
    if latest_window_start is None:
        return float("inf")
    if latest_window_start.tzinfo is None:
        latest_window_start = latest_window_start.replace(tzinfo=UTC)
    age_seconds = (now - latest_window_start).total_seconds()
    return max(0.0, age_seconds - window_seconds)


def freshness_ok(
    latest_window_start: datetime | None,
    now: datetime | None = None,
    window_seconds: int = FRESHNESS_WINDOW_SECONDS,
    max_stale_seconds: float = FRESHNESS_MAX_STALE_SECONDS,
) -> bool:
    """Return True when the curated layer is within the freshness budget."""
    return stale_seconds(latest_window_start, now, window_seconds) <= max_stale_seconds
