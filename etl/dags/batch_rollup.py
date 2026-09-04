"""Airflow DAG: nightly rollup of 1-minute windows (P2-01).

The rollup runs entirely in PostgreSQL over the curated ``page_views_1m``
table and is idempotent (``ON CONFLICT`` upsert), so re-runs are safe.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from etl.dags.clickstream_lib import connect

log = logging.getLogger("airflow.task")

DAILY_ROLLUP_SQL = """
CREATE TABLE IF NOT EXISTS curated.daily_summary (
    day DATE NOT NULL,
    page TEXT NOT NULL,
    device TEXT NOT NULL,
    views BIGINT NOT NULL,
    users BIGINT NOT NULL,
    PRIMARY KEY (day, page, device)
);

INSERT INTO curated.daily_summary (day, page, device, views, users)
SELECT
    (window_start AT TIME ZONE 'UTC')::date AS day,
    page,
    device,
    SUM(views) AS views,
    SUM(users) AS users
FROM curated.page_views_1m
GROUP BY day, page, device
ON CONFLICT (day, page, device)
DO UPDATE SET views = EXCLUDED.views, users = EXCLUDED.users;
"""

DEFAULT_ARGS = {
    "owner": "clickstream",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def run_daily_rollup() -> None:
    """Create/refresh the curated daily summary from 1-minute windows."""
    with connect() as conn, conn.cursor() as cur:
        cur.execute(DAILY_ROLLUP_SQL)
        conn.commit()
    log.info("curated.daily_summary rollup completed")


with DAG(
    dag_id="clickstream_daily_rollup",
    default_args=DEFAULT_ARGS,
    schedule="0 1 * * *",
    start_date=datetime(2026, 9, 2),
    catchup=False,
    tags=["clickstream", "phase2"],
    doc_md=__doc__,
) as dag:
    daily_rollup = PythonOperator(
        task_id="run_daily_rollup",
        python_callable=run_daily_rollup,
    )
