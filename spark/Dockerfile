FROM apache/spark:3.5.7

USER root
RUN pip install --no-cache-dir \
    psycopg[binary]==3.3.5 \
    clickhouse-connect==1.7.2

COPY spark_jobs /opt/clickstream/spark_jobs
ENV PYTHONPATH=/opt/clickstream

USER spark
