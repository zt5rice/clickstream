"""Unit tests for the P2-01 freshness/rollup helpers (no Airflow needed)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from etl.dags.clickstream_lib import freshness_ok, latest_window_sql, stale_seconds


def test_stale_seconds_current_window_is_zero() -> None:
    now = datetime(2026, 9, 2, 6, 54, 30, tzinfo=UTC)
    latest = now.replace(second=0, microsecond=0)
    assert stale_seconds(latest, now) == 0.0


def test_stale_seconds_one_window_behind_is_sixty() -> None:
    now = datetime(2026, 9, 2, 6, 54, 30, tzinfo=UTC)
    latest = now - timedelta(minutes=2)
    assert stale_seconds(latest, now) == pytest.approx(60.0)


def test_stale_seconds_naive_datetime_is_treated_as_utc() -> None:
    now = datetime(2026, 9, 2, 6, 54, 30, tzinfo=UTC)
    latest = datetime(2026, 9, 2, 6, 54, 0)  # naive -> interpreted as UTC
    assert stale_seconds(latest, now) == 0.0


def test_stale_seconds_empty_table_is_infinite() -> None:
    assert stale_seconds(None, datetime(2026, 9, 2, tzinfo=UTC)) == float("inf")


def test_freshness_ok_respects_budget() -> None:
    now = datetime(2026, 9, 2, 6, 54, 30, tzinfo=UTC)
    latest = now - timedelta(minutes=2)
    assert freshness_ok(latest, now, max_stale_seconds=120.0)
    assert not freshness_ok(latest, now, max_stale_seconds=30.0)


def test_latest_window_sql_targets_curated_table() -> None:
    sql = latest_window_sql()
    assert "curated.page_views_1m" in sql
    assert "MAX(window_start)" in sql
