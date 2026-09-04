# Airflow image for the clickstream local demo (P2-01).
# psycopg (v3) is needed by the DAGs to reach the curated Postgres database.
# The official image installs Airflow into the `airflow` user's site-packages
# (PIP_USER is enabled), so extensions must be installed as that same user to
# land next to Airflow/SQLAlchemy and be importable at runtime.
FROM apache/airflow:2.10.5

RUN pip install --no-cache-dir \
    "psycopg[binary]==3.3.5" \
    "psycopg2-binary==2.9.9"

ENV PYTHONPATH=/opt/airflow
