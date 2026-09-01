# ai_assistant — LLM Pipeline-Ops Assistant

Status: scaffold (TODO) · Purpose: hit the "AI-powered capabilities" line in data-platform JDs
(e.g., Sony Staff Software Engineer – Data, R-124982) while reusing the existing NL2SQL / GraphAtlas / MCP experience.

## Planned capabilities

- Explain a Kafka-lag / DLQ / latency alert in plain language (given Prometheus metrics)
- Generate read-only SQL against curated Postgres / ClickHouse tables from a natural-language question
- Recommend a runbook step or RCA outline for a given incident type

## Suggested stack

- Python + FastAPI (port 8001), LangChain or direct LLM API, read-only SQL guard
- Optional: MCP tool integration to query Grafana / ClickHouse / Postgres
- Follow the defense-in-depth + SSE streaming patterns already proven in NL2SQLAgent

## TODO

- [ ] Define API endpoints (health, explain-alert, nl2sql)
- [ ] Read-only SQL guard + schema-aware prompting
- [ ] Tests (pytest) + integration with local Postgres/ClickHouse
- [ ] Prometheus metrics + basic auth
