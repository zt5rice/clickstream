"""FastAPI application: health, readiness, and read-only query endpoints."""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager, suppress
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from . import cache, db
from .config import Settings
from .metrics import (
    http_request_duration_seconds,
    http_requests_total,
    lag_collector_loop,
)
from .ratelimit import rate_limit

settings = Settings()
log = logging.getLogger("api.main")


async def _run_lag_collector() -> None:
    """Run the lag collector and keep the API alive if Kafka is starting."""
    topics = tuple(topic.strip() for topic in settings.kafka_lag_topics.split(",") if topic.strip())
    try:
        await lag_collector_loop(settings, topics, settings.kafka_lag_refresh_seconds)
    except Exception:
        log.exception("kafka lag collector stopped; API continues serving")


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = None
    if settings.kafka_lag_refresh_seconds > 0:
        task = asyncio.create_task(_run_lag_collector())
    yield
    if task is not None:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


app = FastAPI(
    title="clickstream-api",
    description="Read-only REST/JSON API for the clickstream pipeline",
    version="0.1.0",
    lifespan=lifespan,
)


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    http_requests_total.labels(
        method=request.method,
        path=request.url.path,
        status=response.status_code,
    ).inc()
    http_request_duration_seconds.labels(
        method=request.method,
        path=request.url.path,
    ).observe(time.perf_counter() - start)
    return response


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/ready")
def ready() -> dict[str, Any]:
    checks = db.check_ready(settings)
    if not all(checks.values()):
        raise HTTPException(status_code=503, detail=checks)
    return {"status": "ready", "checks": checks}


@app.get("/api/v1/summary", dependencies=[Depends(rate_limit)])
def summary() -> dict[str, Any]:
    return _cached("v1:summary", lambda: _query(db.query_summary))


@app.get("/api/v1/top/pages", dependencies=[Depends(rate_limit)])
def top_pages(limit: int = Query(default=10, ge=1, le=100)) -> dict[str, Any]:
    return {"pages": _cached(f"v1:top:pages:{limit}", lambda: _query(db.query_top_pages, limit))}


@app.get("/api/v1/top/campaigns", dependencies=[Depends(rate_limit)])
def top_campaigns(limit: int = Query(default=10, ge=1, le=100)) -> dict[str, Any]:
    return {
        "campaigns": _cached(
            f"v1:top:campaigns:{limit}", lambda: _query(db.query_top_campaigns, limit)
        )
    }


@app.get("/api/v1/events/recent", dependencies=[Depends(rate_limit)])
def recent_events(limit: int = Query(default=50, ge=1, le=500)) -> dict[str, Any]:
    return {"events": _query(db.query_recent_events, limit)}


@app.get("/api/v1/timeline", dependencies=[Depends(rate_limit)])
def timeline(limit: int = Query(default=100, ge=1, le=1000)) -> dict[str, Any]:
    return {"windows": _query(db.query_timeline, limit)}


@app.get("/api/v1/health/topics", dependencies=[Depends(rate_limit)])
def topics() -> dict[str, Any]:
    return {"topics": _query(db.kafka_topics)}


def _query(func, *args: Any) -> Any:
    try:
        return func(settings, *args)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"backend unavailable: {exc}") from exc


def _cached(key: str, fetcher: Any, ttl_seconds: int | None = None) -> Any:
    """Serve from Redis when present, otherwise compute and populate the cache."""
    ttl = ttl_seconds or settings.cache_ttl_seconds
    hit = cache.get_json(key)
    if hit is not None:
        return hit
    data = fetcher()
    cache.set_json(key, data, ttl)
    return data
