# go_ops — clickstream control-plane CLI

Read-only CLI for the clickstream FastAPI (P2-06, ZHA-145). Pure Go standard
library, no third-party dependencies; script-friendly exit codes.

```bash
make go-ops-test    # run Go unit tests in golang:1.24.3-alpine
make go-ops-build   # build ./go_ops/bin/go_ops (Linux binary)
```

Commands (against the running local API):

```bash
go_ops health    --base-url http://localhost:8000   # ready check
go_ops topics    --base-url http://localhost:8000   # Kafka topics + partitions
go_ops freshness --base-url http://localhost:8000   # latest curated window + stale seconds
go_ops status    --base-url http://localhost:8000   # all of the above
```

Exit code is `1` when the API is unhealthy, unreachable, or the pipeline is
stale beyond 180 seconds.
