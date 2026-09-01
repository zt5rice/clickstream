"""Tests for Spark windowed aggregations (require PySpark + a JVM).

Skipped automatically when PySpark is not installed (local venv/CI default) or
when no Java runtime is available. They run locally once Java + PySpark are
present, and inside the Spark docker image during P1-16 verification.
"""

from __future__ import annotations

import shutil
from datetime import datetime

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from spark_jobs import streaming_job  # noqa: E402

pytestmark = pytest.mark.skipif(shutil.which("java") is None, reason="Java runtime not available")


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("clickstream-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def _events_df(spark: SparkSession):
    rows = [
        ("e1", "page_view", "u-1", "2026-08-27 15:00:05", "/home", "mobile", "organic"),
        ("e2", "page_view", "u-1", "2026-08-27 15:00:20", "/product/1", "mobile", "organic"),
        ("e3", "click", "u-2", "2026-08-27 15:00:40", "/product/1", "desktop", "organic"),
        ("e4", "page_view", "u-2", "2026-08-27 15:01:10", "/cart", "desktop", "c-1"),
        ("e5", "add_to_cart", "u-2", "2026-08-27 15:01:30", "/cart", "desktop", "c-1"),
        ("e6", "purchase", "u-2", "2026-08-27 15:01:50", "/checkout", "desktop", "c-1"),
    ]
    return spark.createDataFrame(
        rows,
        ["event_id", "event_type", "user_id", "ts", "page", "device", "campaign_id"],
    ).withColumn("ts", F.to_timestamp("ts", "yyyy-MM-dd HH:mm:ss"))


def _index_by(rows, keys):
    return {tuple(getattr(row, key) for key in keys): row for row in rows}


def test_page_views_1m(spark):
    result = _index_by(
        streaming_job.page_views_1m(_events_df(spark)).collect(),
        ("window_start", "page", "device"),
    )

    home = result[(datetime(2026, 8, 27, 15, 0), "/home", "mobile")]
    assert (home.views, home.users) == (1, 1)

    product_mobile = result[(datetime(2026, 8, 27, 15, 0), "/product/1", "mobile")]
    assert (product_mobile.views, product_mobile.users) == (1, 1)

    product_desktop = result[(datetime(2026, 8, 27, 15, 0), "/product/1", "desktop")]
    assert (product_desktop.views, product_desktop.users) == (0, 1)

    cart = result[(datetime(2026, 8, 27, 15, 1), "/cart", "desktop")]
    assert (cart.views, cart.users) == (1, 1)


def test_campaign_stats_1m(spark):
    result = _index_by(
        streaming_job.campaign_stats_1m(_events_df(spark)).collect(),
        ("window_start", "campaign_id"),
    )

    organic = result[(datetime(2026, 8, 27, 15, 0), "organic")]
    assert (
        organic.events,
        organic.views,
        organic.clicks,
        organic.conversions,
        organic.users,
    ) == (3, 2, 1, 0, 2)

    c1 = result[(datetime(2026, 8, 27, 15, 1), "c-1")]
    assert (
        c1.events,
        c1.views,
        c1.clicks,
        c1.conversions,
        c1.users,
    ) == (3, 1, 0, 1, 1)
