"""Unit tests for the Kafka producer wrapper (no broker required)."""

import json
from unittest.mock import MagicMock, patch

from producer.kafka_client import ClickstreamProducer, DeliveryTracker
from producer.schema import ClickEvent

_EVENT = ClickEvent(
    event_id="123e4567-e89b-42d3-a456-426614174000",
    event_type="page_view",
    user_id="u-0001",
    session_id="s-abcdef123456",
    ts="2026-08-27T15:00:00Z",
    page="/home",
    device="mobile",
    region="us-west",
    campaign_id="organic",
    referrer="google",
)


def test_producer_passes_reliability_options():
    with patch("producer.kafka_client.Producer") as mock:
        ClickstreamProducer("broker:9092", linger_ms=25, batch_size=1000)
    (opts,) = mock.call_args.args
    assert opts["bootstrap.servers"] == "broker:9092"
    assert opts["acks"] == "all"
    assert opts["enable.idempotence"] is True
    assert opts["compression.type"] == "lz4"
    assert opts["retries"] == 5
    assert opts["linger.ms"] == 25
    assert opts["batch.size"] == 1000


def test_produce_uses_user_key_and_json_value():
    with patch("producer.kafka_client.Producer") as mock:
        producer = ClickstreamProducer("broker:9092")
    producer.produce("clicks.raw", _EVENT)
    mock.return_value.produce.assert_called_once()
    _, kwargs = mock.return_value.produce.call_args
    assert kwargs["key"] == b"u-0001"
    assert json.loads(kwargs["value"]) == _EVENT.to_dict()
    assert callable(kwargs["callback"])


def test_delivery_tracker_counts_success():
    tracker = DeliveryTracker()
    tracker.on_delivery(None, MagicMock())
    assert tracker.delivered == 1
    assert tracker.failed == 0


def test_delivery_tracker_counts_failure():
    tracker = DeliveryTracker()
    tracker.on_delivery(Exception("boom"), MagicMock())
    assert tracker.delivered == 0
    assert tracker.failed == 1
    assert tracker.last_error == "boom"
