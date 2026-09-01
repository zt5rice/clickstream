"""Clickstream event producer: schema, simulator, and (later) Kafka client."""

from .config import ProducerConfig
from .kafka_client import ClickstreamProducer
from .schema import ClickEvent
from .simulator import ClickstreamSimulator

__all__ = ["ClickEvent", "ClickstreamProducer", "ClickstreamSimulator", "ProducerConfig"]
