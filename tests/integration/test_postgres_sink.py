"""Integration test: PostgreSQL sink upsert (skipped when Postgres is down)."""

from datetime import datetime

import psycopg
import pytest

from spark_jobs.postgres_sink import (
    PAGE_VIEWS_COLUMNS,
    PAGE_VIEWS_KEY,
    PAGE_VIEWS_TABLE,
    SCHEMA,
    ensure_schema,
    insert_sql,
    upsert_sql,
)
from tests.integration.services import POSTGRES_DSN, postgres_available

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not postgres_available(),
        reason="Postgres not available on localhost:5432",
    ),
]


def test_page_views_upsert_is_idempotent():
    table = f"{SCHEMA}.{PAGE_VIEWS_TABLE}"
    staging = f"{table}_staging"
    key = (datetime(2026, 8, 27, 15, 0), "/home", "mobile")

    with psycopg.connect(POSTGRES_DSN) as conn:
        ensure_schema(conn)
        conn.execute(
            f"DELETE FROM {table} WHERE window_start = %s AND page = %s AND device = %s",
            key,
        )
        conn.commit()

        for views in (5, 9):
            conn.execute(f"TRUNCATE {staging}")
            conn.execute(insert_sql(staging, PAGE_VIEWS_COLUMNS), (*key, views, 1))
            conn.execute(upsert_sql(staging, table, PAGE_VIEWS_COLUMNS, PAGE_VIEWS_KEY))
        conn.commit()

        row = conn.execute(
            f"SELECT views FROM {table} WHERE window_start = %s AND page = %s AND device = %s",
            key,
        ).fetchone()
        assert row is not None
        assert row[0] == 9
