"""Unit tests for Prometheus metrics (middleware, /metrics, lag computation)."""

from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient
from kafka import OffsetAndMetadata, TopicPartition
from prometheus_client import REGISTRY

from api import main
from api.config import Settings
from api.metrics import (
    fetch_lag,
    kafka_consumer_lag,
    kafka_topic_end_offset,
    refresh_lag_gauges,
)


def test_metrics_endpoint_exposes_prometheus_text():
    client = TestClient(main.app)
    client.get("/health")
    resp = client.get("/metrics")
    assert resp.status_code == 200
    assert "text/plain" in resp.headers["content-type"]
    assert 'http_requests_total{method="GET",path="/health",status="200"} ' in resp.text
    assert "http_request_duration_seconds_count" in resp.text


def test_middleware_records_metrics():
    client = TestClient(main.app)
    client.get("/health")
    value = REGISTRY.get_sample_value(
        "http_requests_total",
        {"method": "GET", "path": "/health", "status": "200"},
    )
    assert value >= 1


def test_fetch_lag_computes_lag_for_committed_partitions():
    consumer = MagicMock()
    consumer.partitions_for_topic.return_value = {0, 1}
    tp0 = TopicPartition("clicks.raw", 0)
    tp1 = TopicPartition("clicks.raw", 1)
    consumer.end_offsets.return_value = {tp0: 100, tp1: 100}
    consumer.committed.side_effect = lambda tp: (
        OffsetAndMetadata(offset=80, metadata="") if tp == tp0 else None
    )

    rows = fetch_lag(consumer, "clicks.raw")
    assert rows == [{"topic": "clicks.raw", "partition": 0, "lag": 20}]


def test_refresh_lag_gauges_sets_values():
    kafka_consumer_lag.clear()
    kafka_topic_end_offset.clear()
    consumer_instance = MagicMock()
    consumer_instance.partitions_for_topic.return_value = {0}
    tp0 = TopicPartition("clicks.raw", 0)
    consumer_instance.end_offsets.return_value = {tp0: 100}
    consumer_instance.committed.return_value = OffsetAndMetadata(offset=70, metadata="")

    with patch("api.metrics.KafkaConsumer", return_value=consumer_instance):
        refresh_lag_gauges(Settings(), ("clicks.raw",))

    value = REGISTRY.get_sample_value(
        "kafka_consumer_lag",
        {"topic": "clicks.raw", "partition": "0"},
    )
    assert value == 30


def test_refresh_lag_gauges_sets_end_offsets():
    kafka_consumer_lag.clear()
    kafka_topic_end_offset.clear()
    consumer_instance = MagicMock()
    consumer_instance.partitions_for_topic.return_value = {0}
    tp0 = TopicPartition("clicks.dlq", 0)
    consumer_instance.end_offsets.return_value = {tp0: 250}
    consumer_instance.committed.return_value = None

    with patch("api.metrics.KafkaConsumer", return_value=consumer_instance):
        refresh_lag_gauges(Settings(), ("clicks.dlq",))

    value = REGISTRY.get_sample_value(
        "kafka_topic_end_offset",
        {"topic": "clicks.dlq", "partition": "0"},
    )
    assert value == 250
