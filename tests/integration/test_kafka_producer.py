"""Integration test: producer -> Kafka roundtrip (skipped when Kafka is down)."""

import json
import uuid

import pytest
from kafka import KafkaConsumer

from producer.kafka_client import ClickstreamProducer
from producer.schema import ClickEvent
from tests.integration.services import KAFKA_BOOTSTRAP_SERVERS, kafka_available

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not kafka_available(),
        reason="Kafka not available on localhost:9092",
    ),
]

TOPIC = f"clicks.integration.{uuid.uuid4().hex[:8]}"


def _event(i: int) -> ClickEvent:
    return ClickEvent(
        event_id=str(uuid.uuid4()),
        event_type="page_view",
        user_id=f"u-{i:04d}",
        session_id=f"s-{uuid.uuid4().hex[:12]}",
        ts="2026-08-27T15:00:00Z",
        page="/home",
        device="mobile",
        region="us-west",
        campaign_id="organic",
        referrer="google",
    )


def test_producer_to_kafka_roundtrip():
    events = [_event(i) for i in range(1, 11)]
    producer = ClickstreamProducer(KAFKA_BOOTSTRAP_SERVERS)
    for event in events:
        producer.produce(TOPIC, event)
    assert producer.flush(timeout=10.0) == 0

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        consumer_timeout_ms=10_000,
    )
    messages = list(consumer)
    consumer.close()

    assert len(messages) == len(events)
    payloads = {json.loads(message.value)["event_id"] for message in messages}
    assert payloads == {event.event_id for event in events}
