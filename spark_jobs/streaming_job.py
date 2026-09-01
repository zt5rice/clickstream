"""Spark Structured Streaming job: consume ``clicks.raw`` and aggregate by 1-minute windows.

P1-04 pipeline: Kafka -> JSON parse (UDF) -> 2-minute watermark ->
1-minute tumbling window aggregations:

* ``page_views_1m``: per window, page, device -> views, distinct users
* ``campaign_stats_1m``: per window, campaign_id -> events, views, clicks,
  conversions, distinct users

Sinks are configurable via ``SPARK_SINK_MODE``:
console | memory | postgres | clickhouse | all | none.

* postgres (P1-05): idempotent ``ON CONFLICT`` upserts via psycopg
* clickhouse (P1-06): raw events -> ``olap.clicks`` and window aggregates ->
  ``olap.page_views_1m`` via clickhouse-connect (ReplacingMergeTree)
* all (P1-11): starts both the postgres and clickhouse sinks in parallel
* DLQ (P1-07): parse failures are routed to ``clicks.dlq`` by a second
  streaming query, so the topic is read twice - an accepted trade-off for
  this demo (see PLAN.md).
"""

from __future__ import annotations

import os
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType, StringType, StructField, StructType

from .parsing import parse_message

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
RAW_TOPIC = os.environ.get("KAFKA_TOPIC_RAW", "clicks.raw")
DLQ_TOPIC = os.environ.get("KAFKA_TOPIC_DLQ", "clicks.dlq")
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

PARSED_RESULT_SCHEMA = StructType(
    [
        StructField("event", PARSED_SCHEMA, True),
        StructField("error", StringType(), True),
    ]
)

_parse_udf = F.udf(parse_message, PARSED_RESULT_SCHEMA)


def create_spark_session(app_name: str = "clickstream-aggregations") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_raw(spark: SparkSession) -> DataFrame:
    """Read the raw Kafka topic as a string column."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", RAW_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
        .select(F.col("value").cast(StringType()).alias("raw"))
    )


def parse_events(raw: DataFrame) -> DataFrame:
    """Parse raw messages and keep valid events (with a 2-minute watermark)."""
    parsed = raw.select(_parse_udf("raw").alias("result"))
    return (
        parsed.where(F.col("result.event").isNotNull())
        .select("result.event.*")
        .select(
            "event_id",
            "user_id",
            "session_id",
            "event_type",
            "page",
            "device",
            "region",
            "campaign_id",
            "referrer",
            F.to_timestamp("ts", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("ts"),
        )
        .withWatermark("ts", f"{WATERMARK_SECONDS} seconds")
    )


def parse_failures(raw: DataFrame) -> DataFrame:
    """Keep unparseable messages for the dead-letter topic."""
    parsed = raw.select(_parse_udf("raw").alias("result"), "raw")
    return parsed.where(F.col("result.error").isNotNull()).select(
        F.col("raw").cast(BinaryType()).alias("value")
    )


def read_events(spark: SparkSession) -> DataFrame:
    """Read, parse, and validate clickstream events from Kafka."""
    return parse_events(read_raw(spark))


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


def start_sinks(events: DataFrame, page_views: DataFrame, campaign_stats: DataFrame) -> list[Any]:
    """Start configured sinks for the parsed-event and aggregation streams."""
    if SINK_MODE not in ("console", "memory", "postgres", "clickhouse", "all", "none"):
        raise ValueError(f"unsupported SPARK_SINK_MODE: {SINK_MODE!r}")
    queries: list[Any] = []
    if SINK_MODE == "postgres":
        return _start_postgres_sinks(page_views, campaign_stats)
    if SINK_MODE == "clickhouse":
        return _start_clickhouse_sinks(events, page_views)
    if SINK_MODE == "all":
        queries = _start_postgres_sinks(page_views, campaign_stats)
        queries.extend(_start_clickhouse_sinks(events, page_views))
        return queries
    if SINK_MODE in ("console", "memory"):
        for name, frame in (("page_views_1m", page_views), ("campaign_stats_1m", campaign_stats)):
            queries.append(_start_query(frame, name))
    return queries


def _start_clickhouse_sinks(events: DataFrame, page_views: DataFrame) -> list[Any]:
    """Start sinks writing raw events and page-view aggregates to ClickHouse."""
    from .clickhouse_sink import ensure_schema, get_client, insert_clicks, insert_page_views

    client = get_client()
    try:
        ensure_schema(client)
    finally:
        client.close()

    clicks_query = (
        events.writeStream.outputMode("append")
        .foreachBatch(insert_clicks)
        .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/clicks_ch")
        .queryName("clicks_ch")
        .start()
    )
    page_views_query = (
        page_views.writeStream.outputMode("update")
        .foreachBatch(insert_page_views)
        .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/page_views_1m_ch")
        .queryName("page_views_1m_ch")
        .start()
    )
    return [clicks_query, page_views_query]


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
    raw = read_raw(spark)
    events = parse_events(raw)
    start_sinks(events, page_views_1m(events), campaign_stats_1m(events))
    start_dlq_sink(parse_failures(raw))
    spark.streams.awaitAnyTermination()


def start_dlq_sink(failures: DataFrame) -> Any | None:
    """Write parse failures to the dead-letter topic (second read of the source)."""
    if SINK_MODE == "none":
        return None
    return (
        failures.writeStream.outputMode("append")
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", DLQ_TOPIC)
        .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/dlq")
        .queryName("clicks_dlq")
        .start()
    )


if __name__ == "__main__":
    run()
