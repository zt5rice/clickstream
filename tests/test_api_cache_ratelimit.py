"""Unit tests for the P2-05 Redis cache and rate limiter (fakeredis)."""

from __future__ import annotations

from decimal import Decimal

from fakeredis import FakeRedis

from api import cache


def test_set_get_json_roundtrip() -> None:
    r = FakeRedis(decode_responses=True)
    cache.set_json("k", {"total_views": 100}, ttl_seconds=10, conn=r)
    assert cache.get_json("k", conn=r) == {"total_views": 100}


def test_get_json_miss_returns_none() -> None:
    r = FakeRedis(decode_responses=True)
    assert cache.get_json("missing", conn=r) is None


def test_set_json_serializes_decimal_values() -> None:
    r = FakeRedis(decode_responses=True)
    cache.set_json("k", {"views": Decimal("348132")}, ttl_seconds=10, conn=r)
    assert cache.get_json("k", conn=r) == {"views": 348132}


def test_rate_limit_allows_up_to_max() -> None:
    r = FakeRedis(decode_responses=True)
    for _ in range(5):
        assert not cache.rate_limit_exceeded("ip:test", max_requests=5, window_seconds=60, conn=r)


def test_rate_limit_blocks_after_max() -> None:
    r = FakeRedis(decode_responses=True)
    for _ in range(5):
        cache.rate_limit_exceeded("ip:test", max_requests=5, window_seconds=60, conn=r)
    assert cache.rate_limit_exceeded("ip:test", max_requests=5, window_seconds=60, conn=r)


def test_rate_limit_resets_after_window() -> None:
    r = FakeRedis(decode_responses=True)
    for _ in range(5):
        cache.rate_limit_exceeded("ip:test", max_requests=5, window_seconds=60, conn=r)
    assert cache.rate_limit_exceeded("ip:test", max_requests=5, window_seconds=60, conn=r)
    r.flushall()
    assert not cache.rate_limit_exceeded("ip:test", max_requests=5, window_seconds=60, conn=r)
