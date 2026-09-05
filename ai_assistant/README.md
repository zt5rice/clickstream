# ai_assistant — LLM pipeline-ops assistant + anomaly detection (P2-08)

Read-only FastAPI service:

- `POST /assistant/explain-alert` — explain an alert (mock by default).
- `POST /assistant/generate-sql` — generate read-only SQL against an
  **allowlisted** table set (mock returns a safe `SELECT`).
- `GET /assistant/anomalies?metric=pipeline_freshness_seconds` — z-score anomaly
  detection over the last N hours of a Prometheus series.

Default provider is `mock` (no API key, public-repo safe). Set
`AI_LLM_PROVIDER=openai-compatible` + `AI_LLM_API_KEY` to use any
OpenAI-compatible `/chat/completions` endpoint; it falls back to mock when the
key is missing.

```bash
docker compose up -d ai-assistant
curl http://localhost:8090/assistant/generate-sql \
  -H 'content-type: application/json' \
  -d '{"table":"curated.daily_page_summary","question":"daily views by page"}'
```
