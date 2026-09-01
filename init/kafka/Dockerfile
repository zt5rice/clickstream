FROM python:3.11.9-slim

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY init /app/init
ENV PYTHONPATH=/app

CMD ["python", "-m", "init.kafka.init_topics"]
