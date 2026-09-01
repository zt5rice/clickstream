"""Unit tests for the producer CLI (parser, overrides, run loop)."""

from unittest.mock import MagicMock

import pytest

from producer import cli as producer_cli
from producer.config import ProducerConfig
from producer.schema import ClickEvent

pytestmark = pytest.mark.slow

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


def test_parser_defaults():
    args = producer_cli.build_parser().parse_args([])
    assert args.bootstrap_servers is None
    assert args.rate is None
    assert args.max_events is None
    assert args.seed is None


def test_main_applies_cli_overrides(monkeypatch):
    captured = {}

    def fake_run(config):
        captured["config"] = config

    monkeypatch.setattr(producer_cli, "run", fake_run)
    producer_cli.main(["--rate", "250", "--max-events", "10", "--seed", "7"])

    config = captured["config"]
    assert config.rate == 250
    assert config.max_events == 10
    assert config.seed == 7
    assert config.bootstrap_servers == "localhost:9092"
    assert config.topic == "clicks.raw"


def test_run_produces_events_and_closes(monkeypatch):
    config = ProducerConfig(rate=1000, max_events=3, duration_seconds=None)
    simulator = MagicMock()
    simulator.iter_events.return_value = iter([_EVENT, _EVENT, _EVENT])
    producer = MagicMock()

    monkeypatch.setattr(producer_cli, "ClickstreamSimulator", lambda seed: simulator)
    monkeypatch.setattr(
        producer_cli,
        "ClickstreamProducer",
        lambda *args, **kwargs: producer,
    )

    producer_cli.run(config)

    assert producer.produce.call_count == 3
    producer.close.assert_called_once()


def test_run_stops_after_max_events(monkeypatch):
    config = ProducerConfig(rate=1000, max_events=1, duration_seconds=None)
    simulator = MagicMock()
    simulator.iter_events.return_value = iter([_EVENT, _EVENT])
    producer = MagicMock()

    monkeypatch.setattr(producer_cli, "ClickstreamSimulator", lambda seed: simulator)
    monkeypatch.setattr(
        producer_cli,
        "ClickstreamProducer",
        lambda *args, **kwargs: producer,
    )

    producer_cli.run(config)

    assert producer.produce.call_count == 1
