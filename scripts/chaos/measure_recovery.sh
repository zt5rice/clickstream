#!/usr/bin/env bash
# Chaos drill helper: delete a workload and measure time-to-Ready (MTTR).
# Usage: measure_recovery.sh <deployment|pod> <name> [namespace]
set -euo pipefail

KIND=${1:?usage: measure_recovery.sh <deployment|pod> <name> [namespace]}
NAME=${2:?missing name}
NS=${3:-clickstream}

echo "CHAOS: deleting $KIND/$NAME in ns=$NS at $(date -u +%FT%TZ)"
start=$(date +%s)
kubectl -n "$NS" delete "$KIND" "$NAME" --wait=false >/dev/null

if [[ "$KIND" == "deployment" ]]; then
  kubectl -n "$NS" rollout status "$KIND/$NAME" --timeout=300s >/dev/null
else
  # Record the old pod's UID, then poll until a *new* pod (different UID)
  # reports Ready. This handles StatefulSets that recreate the pod quickly.
  old_uid=$(kubectl -n "$NS" get "pod/$NAME" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
  for _ in $(seq 1 150); do
    uid=$(kubectl -n "$NS" get "pod/$NAME" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
    ready=$(kubectl -n "$NS" get "pod/$NAME" \
      -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null || true)
    if [[ -n "$uid" && "$uid" != "$old_uid" && "$ready" == "true" ]]; then
      break
    fi
    sleep 2
  done
  uid=$(kubectl -n "$NS" get "pod/$NAME" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
  ready=$(kubectl -n "$NS" get "pod/$NAME" \
    -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null || true)
  if [[ -z "$uid" || "$uid" == "$old_uid" || "$ready" != "true" ]]; then
    echo "ERROR: pod $NAME did not recover in time" >&2
    exit 1
  fi
fi

end=$(date +%s)
echo "RECOVERED: $KIND/$NAME ready at $(date -u +%FT%TZ)"
echo "MTTR: $((end - start))s"
