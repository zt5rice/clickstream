"""Unit tests for init scripts (Postgres/ClickHouse SQL + Kafka topics)."""

from pathlib import Path
from unittest.mock import MagicMock, patch

from confluent_kafka.admin import NewTopic

from init.kafka import init_topics


def _read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def test_postgres_init_sql_matches_sink_schema():
    sql = _read("init/postgres/init.sql")
    assert "CREATE SCHEMA IF NOT EXISTS curated" in sql
    assert "CREATE TABLE IF NOT EXISTS curated.page_views_1m" in sql
    assert "CREATE TABLE IF NOT EXISTS curated.campaign_stats_1m" in sql
    assert "PRIMARY KEY (window_start, page, device)" in sql
    assert "PRIMARY KEY (window_start, campaign_id)" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_page_views_1m_window_start" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_campaign_stats_1m_window_start" in sql


def test_clickhouse_init_sql_matches_sink_schema():
    sql = _read("init/clickhouse/init.sql")
    assert "CREATE DATABASE IF NOT EXISTS olap" in sql
    assert "CREATE TABLE IF NOT EXISTS olap.clicks" in sql
    assert "CREATE TABLE IF NOT EXISTS olap.page_views_1m" in sql
    assert sql.count("ENGINE = ReplacingMergeTree()") == 2
    assert "ORDER BY event_id" in sql
    assert "ORDER BY (window_start, page, device)" in sql


def test_create_topics_uses_three_partitions():
    admin = MagicMock()
    future = MagicMock()
    future.result.return_value = None
    admin.create_topics.return_value = {"clicks.raw": future}

    created = init_topics.create_topics(admin, {"clicks.raw": 3})

    assert created == ["clicks.raw"]
    (new_topic,) = admin.create_topics.call_args.args[0]
    assert isinstance(new_topic, NewTopic)
    assert new_topic.topic == "clicks.raw"
    assert new_topic.num_partitions == 3
    assert new_topic.replication_factor == 1


def test_verify_topics_returns_existing():
    admin = MagicMock()
    admin.list_topics.return_value.topics = {"clicks.raw": object()}

    existing = init_topics.verify_topics(admin, {"clicks.raw": 3, "clicks.dlq": 3})
    assert existing == ["clicks.raw"]


def test_main_reports_ok(capsys):
    with (
        patch.object(init_topics, "AdminClient", return_value=MagicMock()),
        patch.object(init_topics, "create_topics", return_value=["clicks.raw", "clicks.dlq"]),
        patch.object(init_topics, "verify_topics", return_value=["clicks.raw", "clicks.dlq"]),
    ):
        exit_code = init_topics.main(["--bootstrap-servers", "broker:9092"])
    assert exit_code == 0
    assert "OK: topics ready" in capsys.readouterr().out


def test_main_reports_missing_topics(capsys):
    with (
        patch.object(init_topics, "AdminClient", return_value=MagicMock()),
        patch.object(init_topics, "create_topics", return_value=[]),
        patch.object(init_topics, "verify_topics", return_value=[]),
    ):
        exit_code = init_topics.main(["--bootstrap-servers", "broker:9092"])
    assert exit_code == 1
    assert "ERROR: topics not ready" in capsys.readouterr().out
