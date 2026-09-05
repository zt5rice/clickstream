"""FastAPI service for the clickstream AI assistant (P2-08)."""

from __future__ import annotations

import time
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .anomaly import detect_anomalies
from .config import Settings
from .llm import get_llm
from .sql import ALLOWED_TABLES, sanitize_sql

settings = Settings()
llm = get_llm(settings)

app = FastAPI(
    title="clickstream-ai-assistant", description="LLM pipeline-ops assistant", version="0.1.0"
)


class AlertRequest(BaseModel):
    alert_name: str
    value: str
    message: str = ""


class SQLRequest(BaseModel):
    question: str
    table: str = Field(default="curated.daily_page_summary")
    limit: int = Field(default=100, ge=1, le=1000)


class AnomalyResult(BaseModel):
    metric: str
    anomalies: list[dict[str, Any]]
    note: str


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/assistant/explain-alert")
def explain_alert(req: AlertRequest) -> dict[str, str]:
    return {"explanation": llm.explain_alert(req.alert_name, req.value, req.message)}


@app.post("/assistant/generate-sql")
def generate_sql(req: SQLRequest) -> dict[str, str]:
    if req.table not in ALLOWED_TABLES:
        raise HTTPException(status_code=400, detail=f"table {req.table!r} not allowlisted")
    sql = llm.generate_sql(req.table, req.question)
    try:
        safe_sql = sanitize_sql(sql)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"sql": safe_sql, "table": req.table}


@app.get("/assistant/anomalies")
def anomalies(metric: str = "pipeline_freshness_seconds") -> AnomalyResult:
    end = int(time.time())
    start = end - settings.anomaly_hours * 3600
    url = f"{settings.prometheus_url}/api/v1/query_range"
    params = {"query": metric, "start": start, "end": end, "step": 60}
    try:
        with httpx.Client(timeout=15) as client:
            resp = client.get(url, params=params)
            resp.raise_for_status()
            result = resp.json()["data"]["result"]
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"prometheus unavailable: {exc}") from exc

    series: list[float] = []
    for r in result:
        series.extend(float(v) for _, v in r.get("values", []))
    found = detect_anomalies(series, settings.anomaly_threshold, settings.anomaly_min_points)
    return AnomalyResult(
        metric=metric,
        anomalies=[
            {"index": a.index, "value": a.value, "zscore": round(a.zscore, 2)} for a in found
        ],
        note=f"{len(series)} samples scanned; threshold={settings.anomaly_threshold}",
    )
