"""Local Delta Lake lakehouse demo (P2-03).

Runs entirely on local paths - no cloud account. The job:

1. loads the deterministic sample fixture (``sample_data/clicks.sample.json``),
2. writes the events to a Delta table (append/overwrite),
3. builds a daily page summary and upserts it into a second Delta table with
   ``MERGE`` twice to show idempotent merge semantics,
4. reads both tables back and prints row counts.

Delta vs Iceberg decision (documented in ``docs/lakehouse-delta.md``):
Delta was chosen for the demo because it works out of the box with Spark's SQL
engine via ``spark.sql.extensions`` + ``DeltaCatalog`` - no separate catalog
service needed locally - and it gives us ACID transactions, time travel and
``MERGE``. Iceberg is a strong alternative and stays a future option.
"""

from __future__ import annotations

import argparse
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SAMPLE_PATH = os.environ.get("SAMPLE_PATH", "/opt/clickstream/sample_data/clicks.sample.json")
BASE_PATH = os.environ.get("DELTA_BASE_PATH", "/tmp/clickstream-delta")
CLICKS_PATH = f"{BASE_PATH}/clicks"
SUMMARY_PATH = f"{BASE_PATH}/daily_page_summary"
TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"


def build_session() -> SparkSession:
    """Return a Spark session with the Delta SQL extensions enabled."""
    return (
        SparkSession.builder.appName("clickstream-delta-lakehouse")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def load_sample(spark: SparkSession):
    """Load the JSON-lines fixture and parse ``ts`` into a timestamp column."""
    return spark.read.json(SAMPLE_PATH).select(
        "event_id",
        "user_id",
        "page",
        "device",
        F.to_timestamp("ts", TS_FORMAT).alias("ts"),
    )


def write_clicks(clicks) -> None:
    """Write raw events to the Delta clicks table."""
    clicks.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        CLICKS_PATH
    )


def daily_page_summary(clicks):
    """Daily page summary from the sample events."""
    return (
        clicks.withColumn("day", F.date_format(F.col("ts"), "yyyy-MM-dd"))
        .groupBy("day", "page")
        .agg(F.count("*").alias("views"))
        .select("day", "page", "views")
    )


def seed_summary_table(summary) -> None:
    """Create the summary Delta table from the computed summary."""
    summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        SUMMARY_PATH
    )


def merge_summary(spark: SparkSession, summary) -> int:
    """Upsert the summary via Delta MERGE; returns the number of rows after."""
    summary.createOrReplaceTempView("src_daily_page_summary")
    spark.sql(
        f"""
        MERGE INTO delta.`{SUMMARY_PATH}` AS t
        USING src_daily_page_summary AS s
        ON t.day = s.day AND t.page = s.page
        WHEN MATCHED THEN UPDATE SET t.views = s.views
        WHEN NOT MATCHED THEN INSERT (day, page, views)
            VALUES (s.day, s.page, s.views)
        """
    )
    return spark.read.format("delta").load(SUMMARY_PATH).count()


def main() -> None:
    parser = argparse.ArgumentParser(description="Delta Lake lakehouse demo")
    parser.add_argument(
        "mode", nargs="?", default="all", choices=["all", "write", "merge", "verify"]
    )
    args = parser.parse_args()

    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")

    clicks = load_sample(spark)
    clicks_count = clicks.count()
    if args.mode in ("all", "write"):
        write_clicks(clicks)

    summary = daily_page_summary(clicks)
    summary_count = summary.count()

    if args.mode in ("all", "merge"):
        seed_summary_table(summary)
        after_first = merge_summary(spark, summary)
        after_second = merge_summary(spark, summary)
    else:
        after_first = spark.read.format("delta").load(SUMMARY_PATH).count() if summary_count else 0
        after_second = after_first

    stored_clicks = (
        spark.read.format("delta").load(CLICKS_PATH).count()
        if args.mode in ("all", "verify", "write")
        else clicks_count
    )

    result = {
        "sample_events": clicks_count,
        "clicks_rows_in_delta": int(stored_clicks),
        "summary_rows": int(summary_count),
        "summary_rows_after_merge_1": int(after_first),
        "summary_rows_after_merge_2": int(after_second),
        "merge_idempotent": bool(after_first == after_second),
        "delta_table_path": CLICKS_PATH,
    }
    print("DELTA_DEMO_RESULT " + json.dumps(result))
    if not result["merge_idempotent"]:
        raise SystemExit("merge was not idempotent - expected equal row counts")


if __name__ == "__main__":
    main()
