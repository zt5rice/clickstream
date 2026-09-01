"""Kafka producer wrapper with batching, retries, idempotence, and delivery tracking."""

from __future__ import annotations

import logging

from confluent_kafka import Producer

from .schema import ClickEvent

logger = logging.getLogger(__name__)

_DEFAULT_OPTS = {
    "acks": "all",
    "enable.idempotence": True,
    "compression.type": "lz4",
    "retries": 5,
    "retry.backoff.ms": 200,
    "message.timeout.ms": 30_000,
}


class DeliveryTracker:
    """Counts delivered/failed messages reported by librdkafka callbacks."""

    def __init__(self) -> None:
        self.delivered = 0
        self.failed = 0
        self.last_error: str | None = None

    def on_delivery(self, err, msg) -> None:
        if err is not None:
            self.failed += 1
            self.last_error = str(err)
            logger.warning("delivery failed for %s: %s", msg.topic(), err)
        else:
            self.delivered += 1


class ClickstreamProducer:
    """Thin wrapper around confluent_kafka.Producer for clickstream events."""

    def __init__(
        self,
        bootstrap_servers: str,
        *,
        linger_ms: int = 10,
        batch_size: int = 65_536,
        client_id: str = "clickstream-producer",
    ) -> None:
        opts = {
            **_DEFAULT_OPTS,
            "bootstrap.servers": bootstrap_servers,
            "client.id": client_id,
            "linger.ms": linger_ms,
            "batch.size": batch_size,
        }
        self._producer = Producer(opts)
        self._tracker = DeliveryTracker()

    @property
    def tracker(self) -> DeliveryTracker:
        return self._tracker

    def produce(self, topic: str, event: ClickEvent) -> None:
        """Enqueue one event; keyed by user_id for per-user partitioning."""
        self._producer.produce(
            topic,
            key=event.user_id.encode("utf-8"),
            value=event.to_json().encode("utf-8"),
            callback=self._tracker.on_delivery,
        )

    def poll(self, timeout: float = 0.0) -> None:
        self._producer.poll(timeout)

    def flush(self, timeout: float = 10.0) -> int:
        return self._producer.flush(timeout)

    def close(self) -> None:
        self.flush()
