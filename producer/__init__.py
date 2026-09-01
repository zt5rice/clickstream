"""Clickstream event producer: schema, simulator, and (later) Kafka client."""

from .schema import ClickEvent
from .simulator import ClickstreamSimulator

__all__ = ["ClickEvent", "ClickstreamSimulator"]
