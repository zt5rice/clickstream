"""Availability probes for integration tests (short timeouts, no side effects)."""

from __future__ import annotations

import clickhouse_connect
import psycopg
from confluent_kafka.admin import AdminClient

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
POSTGRES_DSN = "host=localhost port=5432 user=click password=click dbname=clickstream"
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = ""
CLICKHOUSE_DATABASE = "olap"


def kafka_available() -> bool:
    try:
        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
        admin.list_topics(timeout=2.0)
        return True
    except Exception:
        return False


def postgres_available() -> bool:
    try:
        with psycopg.connect(POSTGRES_DSN, connect_timeout=2) as conn:
            conn.execute("SELECT 1")
        return True
    except Exception:
        return False


def clickhouse_available() -> bool:
    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE,
        )
        client.command("SELECT 1")
        client.close()
        return True
    except Exception:
        return False
