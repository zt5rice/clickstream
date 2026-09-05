# AI assistant + anomaly detection (P2-08)

Status: implemented in Phase 2 (P2-08, ZHA-147).

## What it does

- **Explain alerts** (`POST /assistant/explain-alert`): turns an Alertmanager
  alert into concise, actionable guidance.
- **Read-only SQL generation** (`POST /assistant/generate-sql`): generates
  `SELECT` statements only, against a hard-coded table allowlist; the response
  is validated again in code before being returned.
- **Anomaly detection** (`GET /assistant/anomalies`): fetches a Prometheus
  series (e.g. `pipeline_freshness_seconds`) for the last N hours and flags
  points whose z-score exceeds a threshold.

## Public-repo safety

The default provider is **mock**: the service runs with no API key and returns
deterministic responses, so everything is reproducible in CI/demos. An
OpenAI-compatible provider is available behind `AI_LLM_PROVIDER` /
`AI_LLM_API_KEY` (read from env, never committed) and falls back to mock when
the key is missing.

## How to run

```bash
docker compose up -d ai-assistant
curl -s http://localhost:8090/health
```

See `ai_assistant/README.md` for API examples.
