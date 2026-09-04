"""Redis-backed JSON cache and fixed-window rate limiting (P2-05).

Redis is optional: when it is unreachable the cache degrades to a no-op and the
rate limiter fails open, so the read-only API still works without it.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from fastapi import Request
from redis import Redis

from .config import Settings

settings = Settings()


def _json_default(value: Any) -> Any:
    """JSON fallback for Decimal/datetime values returned by the DB layer."""
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def client() -> Redis | None:
    """Return a Redis client (lazy; connections happen on first command)."""
    try:
        return Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            db=settings.redis_db,
            decode_responses=True,
            socket_connect_timeout=1,
        )
    except Exception:
        return None


def get_json(key: str, conn: Redis | None = None) -> Any | None:
    r = conn or client()
    if r is None:
        return None
    try:
        raw = r.get(key)
        return json.loads(raw) if raw is not None else None
    except Exception:
        return None


def set_json(key: str, value: Any, ttl_seconds: int = 10, conn: Redis | None = None) -> bool:
    r = conn or client()
    if r is None:
        return False
    try:
        r.set(key, json.dumps(value, default=_json_default), ex=ttl_seconds)
        return True
    except Exception:
        return False


def client_ip(request: Request) -> str:
    """Best-effort client IP honoring the first X-Forwarded-For entry."""
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def rate_limit_exceeded(
    key: str,
    max_requests: int,
    window_seconds: int,
    conn: Redis | None = None,
) -> bool:
    """Fixed-window counter; True once the limit is exceeded."""
    r = conn or client()
    if r is None:
        return False  # fail open when Redis is unavailable
    try:
        count = r.incr(key)
        if count == 1:
            r.expire(key, window_seconds)
        return count > max_requests
    except Exception:
        return False
