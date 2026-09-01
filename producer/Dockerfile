FROM python:3.11.9-slim

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY producer /app/producer
ENV PYTHONPATH=/app

CMD ["python", "-m", "producer"]
