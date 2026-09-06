# GHCR image CI/CD (P3-04)

Status: implemented 2026-09-06 (verified by YAML parse; images already build
locally through docker compose — the workflow runs on GitHub when merged to
`main`).

## Workflow

`.github/workflows/build-images.yml`:

- Triggers: `push` to `main` (doc changes ignored) + `workflow_dispatch`.
- Concurrency: cancels superseded runs for the same ref.
- Builds via `docker/build-push-action` (Buildx + GHCR layer cache) and pushes:

```text
ghcr.io/zt5rice/clickstream/producer:sha-<sha>    (+ :latest)
ghcr.io/zt5rice/clickstream/api:sha-<sha>         (+ :latest)
ghcr.io/zt5rice/clickstream/spark:sha-<sha>       (+ :latest)
ghcr.io/zt5rice/clickstream/etl:sha-<sha>         (+ :latest)
ghcr.io/zt5rice/clickstream/ai-assistant:sha-<sha> (+ :latest)
```

- Permissions: `contents: read`, `packages: write` (GHCR via `GITHUB_TOKEN`).

## Optional EKS deploy (disabled)

The `deploy-eks` job is present but disabled with `if: false`. To enable it
after Phase 3 EKS exists:

1. Set the job condition to `if: github.ref == 'refs/heads/main'`.
2. Add secrets `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and vars
   `AWS_REGION`, `EKS_CLUSTER_NAME` to the repository.
3. It runs `aws eks update-kubeconfig` then `helm upgrade` of the chart,
   pointing `images.api.repository/tag` at the freshly pushed GHCR image.

## Notes

- Image tags are pinned to the commit SHA, so a deployed release is always
  reproducible.
- `.dockerignore` keeps CI contexts lean (no `.git`, `.venv`, tests, docs).
