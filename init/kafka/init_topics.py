"""Create the Kafka topics used by the clickstream pipeline (idempotent).

Topics:
* ``clicks.raw`` - 3 partitions (producer input / Spark consumption)
* ``clicks.dlq`` - 3 partitions (dead-letter for unparseable events)
"""

from __future__ import annotations

import argparse
import time

from confluent_kafka.admin import AdminClient, NewTopic

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
TOPICS = {
    "clicks.raw": 3,
    "clicks.dlq": 3,
}


def create_topics(
    admin: AdminClient,
    topics: dict[str, int],
    timeout_seconds: float = 30.0,
) -> list[str]:
    """Create topics idempotently; return the names reported as created."""
    new_topics = [
        NewTopic(name, num_partitions=partitions, replication_factor=1)
        for name, partitions in topics.items()
    ]
    futures = admin.create_topics(new_topics)
    created = []
    for name, future in futures.items():
        try:
            future.result(timeout_seconds)
            created.append(name)
        except Exception:
            # Topic may already exist; verify_topics() below confirms readiness.
            continue
    return created


def verify_topics(
    admin: AdminClient,
    topics: dict[str, int],
    timeout_seconds: float = 30.0,
) -> list[str]:
    """Return the names of ``topics`` that currently exist on the broker."""
    metadata = admin.list_topics(timeout=timeout_seconds)
    existing = set(metadata.topics)
    return [name for name in topics if name in existing]


def main(argv: list[str] | None = None, timeout_seconds: float = 30.0) -> int:
    parser = argparse.ArgumentParser(description="Create clickstream Kafka topics")
    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP_SERVERS,
        help="Kafka bootstrap servers",
    )
    args = parser.parse_args(argv)
    admin = AdminClient({"bootstrap.servers": args.bootstrap_servers})
    created = create_topics(admin, TOPICS)

    deadline = time.monotonic() + timeout_seconds
    existing: list[str] = []
    while time.monotonic() < deadline:
        existing = verify_topics(admin, TOPICS)
        if len(existing) == len(TOPICS):
            break
        time.sleep(1.0)

    missing = [name for name in TOPICS if name not in existing]
    if missing:
        print(f"ERROR: topics not ready: {missing}")
        return 1
    print(f"OK: topics ready {sorted(existing)} (created: {sorted(created)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
