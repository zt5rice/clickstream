"""Settings for the clickstream AI assistant (P2-08)."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", env_prefix="AI_")

    llm_provider: str = "mock"  # mock | openai-compatible
    llm_model: str = "gpt-4o-mini"
    llm_base_url: str = "https://api.openai.com/v1"
    llm_api_key: str = ""

    prometheus_url: str = "http://localhost:9090"
    clickstream_api_url: str = "http://localhost:8000"

    anomaly_threshold: float = 3.0
    anomaly_min_points: int = 3
    anomaly_hours: int = 6
