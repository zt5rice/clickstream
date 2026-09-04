"""Prometheus metrics for the API: HTTP latency/counters and Kafka lag gauges."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
from kafka import KafkaConsumer, TopicPartition
from prometheus_client import Counter, Gauge, Histogram

from .config import Settings

logger = logging.getLogger(__name__)

http_requests_total = Counter(
    "http_requests_total",
    "Total HTTP requests handled by the API",
    ("method", "path", "status"),
)
http_request_duration_seconds = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency in seconds",
    ("method", "path"),
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)
kafka_consumer_lag = Gauge(
    "kafka_consumer_lag",
    "Kafka consumer lag per topic partition",
    ("topic", "partition"),
)
kafka_topic_end_offset = Gauge(
    "kafka_topic_end_offset",
    "Kafka end offset per topic partition (DLQ depth when topic is unconsumed)",
    ("topic", "partition"),
)
pipeline_freshness_seconds = Gauge(
    "pipeline_freshness_seconds",
    "Seconds since the most recent curated 1-minute window was written (Spark sink freshness)",
)


def fetch_lag(consumer: KafkaConsumer, topic: str) -> list[dict[str, Any]]:
    """Return ``[{topic, partition, lag}]`` for one topic, best effort."""
    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        return []
    topic_partitions = [TopicPartition(topic, partition) for partition in sorted(partitions)]
    end_offsets = consumer.end_offsets(topic_partitions)
    result = []
    for tp in topic_partitions:
        committed = consumer.committed(tp)
        end = end_offsets.get(tp)
        if committed is not None and end is not None:
            result.append(
                {
                    "topic": topic,
                    "partition": tp.partition,
                    "lag": max(0, end - committed.offset),
                }
            )
    return result


def refresh_lag_gauges(settings: Settings, topics: tuple[str, ...]) -> None:
    """Poll consumer lag for ``topics`` and update the gauge values."""
    consumer = KafkaConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        enable_auto_commit=False,
        consumer_timeout_ms=2000,
    )
    try:
        for topic in topics:
            _set_topic_end_offsets(consumer, topic)
            for row in fetch_lag(consumer, topic):
                kafka_consumer_lag.labels(
                    topic=row["topic"],
                    partition=row["partition"],
                ).set(row["lag"])
    finally:
        consumer.close()


def _set_topic_end_offsets(consumer: KafkaConsumer, topic: str) -> None:
    """Expose the end offset of each partition (used for DLQ depth)."""
    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        return
    topic_partitions = [TopicPartition(topic, partition) for partition in sorted(partitions)]
    end_offsets = consumer.end_offsets(topic_partitions)
    for tp in topic_partitions:
        end = end_offsets.get(tp)
        if end is not None:
            kafka_topic_end_offset.labels(topic=topic, partition=tp.partition).set(end)


async def lag_collector_loop(
    settings: Settings,
    topics: tuple[str, ...],
    interval_seconds: float,
) -> None:
    """Periodically refresh Kafka lag gauges until the task is cancelled."""
    while True:
        try:
            refresh_lag_gauges(settings, topics)
        except Exception:
            logger.exception("failed to refresh Kafka lag gauges")
        try:
            refresh_freshness_gauge(settings)
        except Exception:
            logger.exception("failed to refresh pipeline freshness gauge")
        await asyncio.sleep(interval_seconds)


def refresh_freshness_gauge(settings: Settings) -> None:
    """Set pipeline freshness from the latest curated window in PostgreSQL."""
    with psycopg.connect(settings.postgres_dsn()) as conn:
        row = conn.execute("SELECT MAX(window_start) FROM curated.page_views_1m").fetchone()
    latest = row[0]
    if latest is None:
        pipeline_freshness_seconds.set(0)
        return
    # window_start is the window start; add one minute for the window end.
    now = datetime.now(UTC).replace(tzinfo=None)
    freshness = (now - (latest + timedelta(minutes=1))).total_seconds()
    pipeline_freshness_seconds.set(max(0.0, freshness))
