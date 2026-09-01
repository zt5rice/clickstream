"""FastAPI application: health, readiness, and read-only query endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException, Query

from . import db
from .config import Settings

settings = Settings()

app = FastAPI(
    title="clickstream-api",
    description="Read-only REST/JSON API for the clickstream pipeline",
    version="0.1.0",
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/ready")
def ready() -> dict[str, Any]:
    checks = db.check_ready(settings)
    if not all(checks.values()):
        raise HTTPException(status_code=503, detail=checks)
    return {"status": "ready", "checks": checks}


@app.get("/api/v1/summary")
def summary() -> dict[str, Any]:
    return _query(db.query_summary)


@app.get("/api/v1/top/pages")
def top_pages(limit: int = Query(default=10, ge=1, le=100)) -> dict[str, Any]:
    return {"pages": _query(db.query_top_pages, limit)}


@app.get("/api/v1/top/campaigns")
def top_campaigns(limit: int = Query(default=10, ge=1, le=100)) -> dict[str, Any]:
    return {"campaigns": _query(db.query_top_campaigns, limit)}


@app.get("/api/v1/events/recent")
def recent_events(limit: int = Query(default=50, ge=1, le=500)) -> dict[str, Any]:
    return {"events": _query(db.query_recent_events, limit)}


@app.get("/api/v1/timeline")
def timeline(limit: int = Query(default=100, ge=1, le=1000)) -> dict[str, Any]:
    return {"windows": _query(db.query_timeline, limit)}


@app.get("/api/v1/health/topics")
def topics() -> dict[str, Any]:
    return {"topics": _query(db.kafka_topics)}


def _query(func, *args: Any) -> Any:
    try:
        return func(settings, *args)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"backend unavailable: {exc}") from exc
