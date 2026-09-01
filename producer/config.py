"""Environment-based configuration for the clickstream producer."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "clicks.raw"
DEFAULT_RATE = 100
DEFAULT_DURATION_SECONDS = 600
DEFAULT_MAX_EVENTS = 100_000
DEFAULT_SEED = 42


@dataclass(frozen=True, slots=True)
class ProducerConfig:
    """Producer settings; values come from the environment with sane defaults."""

    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS
    topic: str = DEFAULT_TOPIC
    rate: int = DEFAULT_RATE
    duration_seconds: int | None = DEFAULT_DURATION_SECONDS
    max_events: int = DEFAULT_MAX_EVENTS
    seed: int = DEFAULT_SEED
    linger_ms: int = 10
    batch_size: int = 65_536

    def __post_init__(self) -> None:
        if self.rate <= 0:
            raise ValueError("rate must be > 0")
        if self.duration_seconds is not None and self.duration_seconds < 0:
            raise ValueError("duration_seconds must be >= 0 or None")
        if self.max_events < 0:
            raise ValueError("max_events must be >= 0")
        if self.linger_ms < 0:
            raise ValueError("linger_ms must be >= 0")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be > 0")

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> ProducerConfig:
        env = os.environ if env is None else env
        duration_raw = int(env.get("PRODUCER_DURATION", DEFAULT_DURATION_SECONDS))
        max_events_raw = int(env.get("PRODUCER_MAX_EVENTS", DEFAULT_MAX_EVENTS))
        return cls(
            bootstrap_servers=env.get("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS),
            topic=env.get("PRODUCER_TOPIC", DEFAULT_TOPIC),
            rate=int(env.get("PRODUCER_RATE", DEFAULT_RATE)),
            duration_seconds=None if duration_raw == 0 else duration_raw,
            max_events=0 if max_events_raw == 0 else max_events_raw,
            seed=int(env.get("PRODUCER_SEED", DEFAULT_SEED)),
        )
