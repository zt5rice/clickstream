"""Integration test: ClickHouse sink dedup (skipped when ClickHouse is down)."""

import uuid
from datetime import datetime

import pytest

from spark_jobs.clickhouse_sink import CLICKS_COLUMNS, ensure_schema, get_client
from tests.integration.services import clickhouse_available

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not clickhouse_available(),
        reason="ClickHouse not available on localhost:8123",
    ),
]


def test_clicks_insert_dedups_by_event_id():
    client = get_client()
    try:
        ensure_schema(client)
        event_id = f"it-{uuid.uuid4().hex[:12]}"
        row = (
            event_id,
            "page_view",
            "u-0001",
            "s-abc",
            datetime(2026, 8, 27, 15, 0),
            "/home",
            "mobile",
            "us-west",
            "organic",
            "google",
        )
        client.insert("olap.clicks", [row, row], column_names=list(CLICKS_COLUMNS))

        result = client.query(
            "SELECT count() FROM olap.clicks FINAL WHERE event_id = {event_id:String}",
            parameters={"event_id": event_id},
        )
        assert result.result_rows[0][0] == 1
    finally:
        client.close()
