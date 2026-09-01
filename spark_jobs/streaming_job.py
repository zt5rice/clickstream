"""Spark Structured Streaming job: consume ``clicks.raw`` and aggregate by 1-minute windows.

P1-04 pipeline: Kafka -> JSON parse (UDF) -> 2-minute watermark ->
1-minute tumbling window aggregations:

* ``page_views_1m``: per window, page, device -> views, distinct users
* ``campaign_stats_1m``: per window, campaign_id -> events, views, clicks,
  conversions, distinct users

Sinks are configurable via ``SPARK_SINK_MODE`` (console | memory | postgres | none).
PostgreSQL sink (P1-05) uses idempotent ``ON CONFLICT`` upserts via psycopg;
ClickHouse sink lands in P1-06; DLQ routing in P1-07.
"""

from __future__ import annotations

import os
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from .parsing import ParseError, parse_event

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
RAW_TOPIC = os.environ.get("KAFKA_TOPIC_RAW", "clicks.raw")
WINDOW_MINUTES = 1
WATERMARK_SECONDS = 120
SINK_MODE = os.environ.get("SPARK_SINK_MODE", "console")
CHECKPOINT_LOCATION = os.environ.get("SPARK_CHECKPOINT_LOCATION", "/tmp/clickstream-checkpoints")

PARSED_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("ts", StringType(), False),
        StructField("page", StringType(), False),
        StructField("device", StringType(), False),
        StructField("region", StringType(), False),
        StructField("campaign_id", StringType(), False),
        StructField("referrer", StringType(), False),
    ]
)


def _safe_parse(raw: str | None) -> dict[str, str] | None:
    """Parse one raw Kafka message; return None for unparseable input (DLQ in P1-07)."""
    if raw is None:
        return None
    try:
        return parse_event(raw)
    except ParseError:
        return None


_parse_udf = F.udf(_safe_parse, PARSED_SCHEMA)


def create_spark_session(app_name: str = "clickstream-aggregations") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_events(spark: SparkSession) -> DataFrame:
    """Read and parse clickstream events from Kafka."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", RAW_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
        .select(F.col("value").cast(StringType()).alias("raw"))
    )
    return (
        raw.select(_parse_udf("raw").alias("event"))
        .where(F.col("event").isNotNull())
        .select("event.*")
        .select(
            "event_id",
            "user_id",
            "session_id",
            "event_type",
            "page",
            "device",
            "campaign_id",
            F.to_timestamp("ts", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("ts"),
        )
        .withWatermark("ts", f"{WATERMARK_SECONDS} seconds")
    )


def page_views_1m(events: DataFrame) -> DataFrame:
    """1-minute windowed page views grouped by page and device."""
    return (
        events.groupBy(F.window("ts", f"{WINDOW_MINUTES} minute"), "page", "device")
        .agg(
            F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("views"),
            F.countDistinct("user_id").alias("users"),
        )
        .select(F.col("window.start").alias("window_start"), "page", "device", "views", "users")
    )


def campaign_stats_1m(events: DataFrame) -> DataFrame:
    """1-minute windowed campaign statistics."""
    return (
        events.groupBy(F.window("ts", f"{WINDOW_MINUTES} minute"), "campaign_id")
        .agg(
            F.count("*").alias("events"),
            F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("views"),
            F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias("clicks"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("conversions"),
            F.countDistinct("user_id").alias("users"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            "campaign_id",
            "events",
            "views",
            "clicks",
            "conversions",
            "users",
        )
    )


def start_sinks(page_views: DataFrame, campaign_stats: DataFrame) -> list[Any]:
    """Start configured sinks; ClickHouse sink lands in P1-06."""
    if SINK_MODE not in ("console", "memory", "postgres", "none"):
        raise ValueError(f"unsupported SPARK_SINK_MODE: {SINK_MODE!r}")
    queries: list[Any] = []
    if SINK_MODE == "postgres":
        return _start_postgres_sinks(page_views, campaign_stats)
    if SINK_MODE in ("console", "memory"):
        for name, frame in (("page_views_1m", page_views), ("campaign_stats_1m", campaign_stats)):
            queries.append(_start_query(frame, name))
    return queries


def _start_postgres_sinks(page_views: DataFrame, campaign_stats: DataFrame) -> list[Any]:
    """Start foreachBatch sinks that upsert into curated PostgreSQL tables."""
    from .postgres_sink import upsert_campaign_stats, upsert_page_views

    page_views_query = (
        page_views.writeStream.outputMode("update")
        .foreachBatch(upsert_page_views)
        .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/page_views_1m_pg")
        .queryName("page_views_1m_pg")
        .start()
    )
    campaign_stats_query = (
        campaign_stats.writeStream.outputMode("update")
        .foreachBatch(upsert_campaign_stats)
        .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/campaign_stats_1m_pg")
        .queryName("campaign_stats_1m_pg")
        .start()
    )
    return [page_views_query, campaign_stats_query]


def _start_query(frame: DataFrame, name: str) -> Any:
    checkpoint = f"{CHECKPOINT_LOCATION}/{name}"
    if SINK_MODE == "console":
        return (
            frame.writeStream.outputMode("update")
            .format("console")
            .option("truncate", "false")
            .option("checkpointLocation", checkpoint)
            .queryName(name)
            .start()
        )
    return (
        frame.writeStream.outputMode("update")
        .format("memory")
        .option("checkpointLocation", checkpoint)
        .queryName(name)
        .start()
    )


def run() -> None:
    spark = create_spark_session()
    events = read_events(spark)
    start_sinks(page_views_1m(events), campaign_stats_1m(events))
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run()
