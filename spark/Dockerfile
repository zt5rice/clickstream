# python3-ubuntu variant: Python 3.10 (the plain 3.5.7 image ships Python 3.8,
# which cannot install psycopg 3.3 / clickhouse-connect 1.7).
FROM apache/spark:3.5.7-scala2.12-java17-python3-ubuntu

USER root
RUN pip install --no-cache-dir \
    psycopg[binary]==3.3.5 \
    clickhouse-connect==1.7.2

COPY spark_jobs /opt/clickstream/spark_jobs
COPY sample_data /opt/clickstream/sample_data
ENV PYTHONPATH=/opt/clickstream
# The spark user's default HOME (/nonexistent) is not writable, which breaks
# spark-submit --packages (ivy cache). Point HOME at /tmp instead.
ENV HOME=/tmp

USER spark
