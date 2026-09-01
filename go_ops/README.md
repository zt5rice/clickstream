# go_ops — Go Ops CLI / Control-Plane Service

Status: scaffold (TODO) · Purpose: Go component for infra automation / ops tooling
(hits "Go + infrastructure automation" lines in JDs, e.g., Sony , and Apple Golang roles).

## Planned capabilities

- `go_ops status` — health check + Kafka topic/consumer-group status CLI
- `go_ops lag` — consumer lag summary (reads Prometheus or Kafka admin API)
- Optional: small control-plane HTTP service (port 8002) exposing status endpoints

## Suggested stack

- Go 1.22+, standard library + kafka-go / franz-go, cobra for CLI
- Prometheus client metrics for the control-plane service
- Tests (go test) + GitHub Actions job (go vet, go test, build)

## TODO

- [ ] Scaffold Go module + CLI skeleton
- [ ] Kafka admin: list topics, consumer groups, lag
- [ ] Health-check endpoint + Prometheus metrics
- [ ] Unit tests + CI
