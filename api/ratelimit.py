"""FastAPI dependency that rate-limits read endpoints per client IP (P2-05)."""

from __future__ import annotations

from fastapi import HTTPException, Request

from .cache import client_ip, rate_limit_exceeded
from .config import Settings

settings = Settings()


def rate_limit(request: Request) -> None:
    """Reject with 429 when the client exceeds the fixed-window limit."""
    key = f"ratelimit:{client_ip(request)}"
    exceeded = rate_limit_exceeded(
        key,
        settings.rate_limit_max_requests,
        settings.rate_limit_window_seconds,
    )
    if exceeded:
        raise HTTPException(
            status_code=429,
            detail="rate limit exceeded",
            headers={"Retry-After": str(settings.rate_limit_window_seconds)},
        )
