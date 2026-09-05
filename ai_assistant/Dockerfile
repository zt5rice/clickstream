FROM python:3.11.9-slim

WORKDIR /app
COPY ai_assistant/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY ai_assistant /app/ai_assistant
ENV PYTHONPATH=/app

EXPOSE 8090
CMD ["uvicorn", "ai_assistant.main:app", "--host", "0.0.0.0", "--port", "8090"]
