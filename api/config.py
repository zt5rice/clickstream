"""API settings loaded from the environment (``.env`` supported)."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    api_port: int = 8000

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_user: str = "click"
    postgres_password: str = "click"
    postgres_db: str = "clickstream"

    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "click"
    clickhouse_password: str = "click"
    clickhouse_database: str = "olap"

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_consumer_group: str = "clickstream-spark"
    kafka_lag_refresh_seconds: int = 15
    kafka_lag_topics: str = "clicks.raw,clicks.dlq"

    def postgres_dsn(self) -> str:
        return (
            f"host={self.postgres_host} port={self.postgres_port} "
            f"user={self.postgres_user} password={self.postgres_password} "
            f"dbname={self.postgres_db}"
        )
