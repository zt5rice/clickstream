"""Read-only database access for the API (PostgreSQL, ClickHouse, Kafka)."""

from __future__ import annotations

from typing import Any

import clickhouse_connect
import psycopg
from kafka import KafkaAdminClient, KafkaConsumer

from .config import Settings


def check_ready(settings: Settings) -> dict[str, bool]:
    """Probe each dependency and report per-service availability."""
    return {
        "postgres": _postgres_ok(settings),
        "clickhouse": _clickhouse_ok(settings),
        "kafka": _kafka_ok(settings),
    }


def _postgres_ok(settings: Settings) -> bool:
    try:
        with psycopg.connect(settings.postgres_dsn()) as conn:
            conn.execute("SELECT 1")
        return True
    except Exception:
        return False


def _clickhouse_ok(settings: Settings) -> bool:
    try:
        client = _clickhouse_client(settings)
        try:
            client.command("SELECT 1")
            return True
        finally:
            client.close()
    except Exception:
        return False


def _kafka_ok(settings: Settings) -> bool:
    try:
        admin = _kafka_admin(settings)
        try:
            admin.list_topics()
            return True
        finally:
            admin.close()
    except Exception:
        return False


def query_summary(settings: Settings) -> dict[str, Any]:
    """Aggregate high-level metrics from the curated PostgreSQL tables."""
    with psycopg.connect(settings.postgres_dsn()) as conn:
        row = conn.execute(
            """
            SELECT
                COALESCE(SUM(views), 0) AS total_views,
                COALESCE(SUM(users), 0) AS total_users,
                COUNT(DISTINCT page) AS pages,
                MAX(window_start) AS latest_window
            FROM curated.page_views_1m
            """
        ).fetchone()
        campaigns = conn.execute(
            "SELECT COUNT(DISTINCT campaign_id) FROM curated.campaign_stats_1m"
        ).fetchone()[0]
    return {
        "total_views": row[0],
        "total_users": row[1],
        "pages": row[2],
        "campaigns": campaigns,
        "latest_window": row[3].isoformat() if row[3] is not None else None,
    }


def query_top_pages(settings: Settings, limit: int) -> list[dict[str, Any]]:
    """Top pages by total views from the curated page_views_1m table."""
    with psycopg.connect(settings.postgres_dsn()) as conn:
        rows = conn.execute(
            """
            SELECT page, SUM(views) AS views, SUM(users) AS users
            FROM curated.page_views_1m
            GROUP BY page
            ORDER BY views DESC, page
            LIMIT %s
            """,
            (limit,),
        ).fetchall()
    return [{"page": row[0], "views": row[1], "users": row[2]} for row in rows]


def query_top_campaigns(settings: Settings, limit: int) -> list[dict[str, Any]]:
    """Top campaigns by event volume from the curated campaign_stats_1m table."""
    with psycopg.connect(settings.postgres_dsn()) as conn:
        rows = conn.execute(
            """
            SELECT
                campaign_id,
                SUM(events) AS events,
                SUM(views) AS views,
                SUM(clicks) AS clicks,
                SUM(conversions) AS conversions
            FROM curated.campaign_stats_1m
            GROUP BY campaign_id
            ORDER BY events DESC, campaign_id
            LIMIT %s
            """,
            (limit,),
        ).fetchall()
    return [
        {
            "campaign_id": row[0],
            "events": row[1],
            "views": row[2],
            "clicks": row[3],
            "conversions": row[4],
        }
        for row in rows
    ]


def query_recent_events(settings: Settings, limit: int) -> list[dict[str, Any]]:
    """Most recent raw events from ClickHouse (deduped with FINAL)."""
    client = _clickhouse_client(settings)
    try:
        rows = client.query(
            f"""
            SELECT event_id, event_type, user_id, page, device, region,
                   campaign_id, referrer, ts
            FROM olap.clicks FINAL
            ORDER BY ts DESC
            LIMIT {limit}
            """
        ).result_rows
    finally:
        client.close()
    columns = (
        "event_id",
        "event_type",
        "user_id",
        "page",
        "device",
        "region",
        "campaign_id",
        "referrer",
        "ts",
    )
    return [dict(zip(columns, row, strict=True)) for row in rows]


def query_timeline(settings: Settings, limit: int) -> list[dict[str, Any]]:
    """Most recent 1-minute windows from ClickHouse (deduped with FINAL)."""
    client = _clickhouse_client(settings)
    try:
        rows = client.query(
            f"""
            SELECT window_start, page, device, views, users
            FROM olap.page_views_1m FINAL
            ORDER BY window_start DESC
            LIMIT {limit}
            """
        ).result_rows
    finally:
        client.close()
    columns = ("window_start", "page", "device", "views", "users")
    return [dict(zip(columns, row, strict=True)) for row in rows]


def kafka_topics(settings: Settings) -> list[dict[str, Any]]:
    """List Kafka topics with their partition counts."""
    consumer = KafkaConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        enable_auto_commit=False,
    )
    try:
        names = sorted(consumer.topics())
    finally:
        consumer.close()
    result = []
    for name in names:
        # partitions_for_topic requires a live consumer; use a fresh consumer
        # per lookup via the same connection semantics.
        partitions = _topic_partitions(settings, name)
        result.append({"topic": name, "partitions": partitions})
    return result


def _topic_partitions(settings: Settings, topic: str) -> int:
    consumer = KafkaConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        enable_auto_commit=False,
    )
    try:
        return len(consumer.partitions_for_topic(topic) or set())
    finally:
        consumer.close()


def _clickhouse_client(settings: Settings):
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
    )


def _kafka_admin(settings: Settings) -> KafkaAdminClient:
    return KafkaAdminClient(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        client_id="clickstream-api",
    )
