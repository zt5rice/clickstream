"""Airflow DAG: alert when curated data is stale (P2-01).

Checks the latest curated 1-minute window in Postgres every 15 minutes and
fails the run when the pipeline is behind by more than
``FRESHNESS_MAX_STALE_SECONDS`` (default 180s).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

from etl.dags.clickstream_lib import (
    FRESHNESS_MAX_STALE_SECONDS,
    connect,
    fetch_latest_window,
    stale_seconds,
)

log = logging.getLogger("airflow.task")

DEFAULT_ARGS = {
    "owner": "clickstream",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def check_curated_freshness() -> float:
    """Return the current stale-seconds value; raise when past the budget."""
    with connect() as conn:
        latest = fetch_latest_window(conn)
    stale = stale_seconds(latest)
    log.info(
        "latest curated window=%s stale_seconds=%.1f max=%.1f",
        latest,
        stale,
        FRESHNESS_MAX_STALE_SECONDS,
    )
    if stale > FRESHNESS_MAX_STALE_SECONDS:
        raise AirflowException(
            "curated data is stale: "
            f"latest_window={latest}, stale_seconds={stale:.1f} > "
            f"{FRESHNESS_MAX_STALE_SECONDS}"
        )
    return stale


with DAG(
    dag_id="clickstream_freshness_check",
    default_args=DEFAULT_ARGS,
    schedule="*/15 * * * *",
    start_date=datetime(2026, 9, 2),
    catchup=False,
    tags=["clickstream", "phase2"],
    doc_md=__doc__,
) as dag:
    check_freshness = PythonOperator(
        task_id="check_curated_freshness",
        python_callable=check_curated_freshness,
    )
