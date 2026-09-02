"""Command-line entry point for the clickstream producer."""

from __future__ import annotations

import argparse
import logging
import time
from dataclasses import replace

from .config import ProducerConfig
from .kafka_client import ClickstreamProducer
from .simulator import ClickstreamSimulator

LOG_INTERVAL_SECONDS = 5.0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate clickstream events to Kafka")
    parser.add_argument(
        "--bootstrap-servers",
        help="Kafka bootstrap servers (env: KAFKA_BOOTSTRAP_SERVERS)",
    )
    parser.add_argument("--topic", help="Kafka topic (env: PRODUCER_TOPIC)")
    parser.add_argument("--rate", type=int, help="events per second (env: PRODUCER_RATE)")
    parser.add_argument(
        "--duration",
        type=int,
        help="run duration in seconds, 0 = unlimited (env: PRODUCER_DURATION)",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        help="stop after N events, 0 = unlimited (env: PRODUCER_MAX_EVENTS)",
    )
    parser.add_argument("--seed", type=int, help="random seed (env: PRODUCER_SEED)")
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args(argv)
    config = ProducerConfig.from_env()
    overrides = {
        "bootstrap_servers": args.bootstrap_servers,
        "topic": args.topic,
        "rate": args.rate,
        "duration_seconds": args.duration,
        "max_events": args.max_events,
        "seed": args.seed,
    }
    config = replace(
        config,
        **{key: value for key, value in overrides.items() if value is not None},
    )
    run(config)
    return 0


def run(config: ProducerConfig) -> None:
    logging.info(
        "starting producer: servers=%s topic=%s rate=%s/s seed=%s duration=%s max_events=%s",
        config.bootstrap_servers,
        config.topic,
        config.rate,
        config.seed,
        config.duration_seconds,
        config.max_events,
    )
    simulator = ClickstreamSimulator(
        seed=config.seed,
        events_per_second=config.rate,
    )
    producer = ClickstreamProducer(
        config.bootstrap_servers,
        linger_ms=config.linger_ms,
        batch_size=config.batch_size,
    )

    start = time.monotonic()
    interval = 1.0 / config.rate
    deadline = start + config.duration_seconds if config.duration_seconds else None
    produced = 0
    next_log = start + LOG_INTERVAL_SECONDS
    for event in simulator.iter_events():
        if config.max_events and produced >= config.max_events:
            break
        if deadline is not None and time.monotonic() >= deadline:
            break
        producer.produce(config.topic, event)
        producer.poll(0)
        produced += 1
        target = start + produced * interval
        sleep = target - time.monotonic()
        if sleep > 0:
            time.sleep(sleep)
        if time.monotonic() >= next_log:
            _log_progress(producer, produced, start)
            next_log = time.monotonic() + LOG_INTERVAL_SECONDS

    producer.close()
    elapsed = time.monotonic() - start
    logging.info(
        "done: produced=%d delivered=%d failed=%d elapsed=%.1fs avg_rate=%.1f ev/s",
        produced,
        producer.tracker.delivered,
        producer.tracker.failed,
        elapsed,
        produced / elapsed if elapsed else 0.0,
    )


def _log_progress(producer: ClickstreamProducer, produced: int, start: float) -> None:
    elapsed = time.monotonic() - start
    logging.info(
        "progress: produced=%d rate=%.1f ev/s delivered=%d failed=%d",
        produced,
        produced / elapsed if elapsed else 0.0,
        producer.tracker.delivered,
        producer.tracker.failed,
    )
