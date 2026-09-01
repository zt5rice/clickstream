"""Unit tests for producer environment-based configuration."""

import pytest

from producer.config import ProducerConfig


def test_defaults():
    config = ProducerConfig.from_env({})
    assert config.bootstrap_servers == "localhost:9092"
    assert config.topic == "clicks.raw"
    assert config.rate == 100
    assert config.duration_seconds == 600
    assert config.max_events == 100_000
    assert config.seed == 42


def test_env_overrides():
    env = {
        "KAFKA_BOOTSTRAP_SERVERS": "broker:9092",
        "PRODUCER_TOPIC": "clicks.test",
        "PRODUCER_RATE": "250",
        "PRODUCER_DURATION": "30",
        "PRODUCER_MAX_EVENTS": "5000",
        "PRODUCER_SEED": "7",
    }
    config = ProducerConfig.from_env(env)
    assert config.bootstrap_servers == "broker:9092"
    assert config.topic == "clicks.test"
    assert config.rate == 250
    assert config.duration_seconds == 30
    assert config.max_events == 5000
    assert config.seed == 7


def test_zero_means_unlimited():
    config = ProducerConfig.from_env({"PRODUCER_DURATION": "0", "PRODUCER_MAX_EVENTS": "0"})
    assert config.duration_seconds is None
    assert config.max_events == 0


@pytest.mark.parametrize("env", [{"PRODUCER_RATE": "0"}, {"PRODUCER_RATE": "-5"}])
def test_invalid_rate_rejected(env):
    with pytest.raises(ValueError):
        ProducerConfig.from_env(env)
