# Clickstream Project — Collaboration, Ethics & Resources

Status: v0.2 · Scope: decision notes for a personal, resume-building clickstream project.

## 1. Public-repo guardrails

This repository must stay **public-repo safe**:

- All code is original and generic (industry-standard technologies only).
- No proprietary code, configs, internal tool names, internal documents, or
  employer metrics/data of any kind.
- If collaborators are involved: no company time, company accounts, or company
  laptops; everyone keeps their own employment/NDA obligations.
- Present the project as a **personal / collaborative portfolio project**, never
  as anyone's production work.

## 2. Toy project vs. actual production

- This is a **small, independent, from-scratch demo** built with public/standard
  technologies (Kafka, Spark, EKS-ready, etc.).
- Scale and compute are intentionally tiny; claims are backed only by our own
  measured runs.
- Resume rule: always label it **personal / portfolio**, with our own measured
  metrics (throughput, latency, lag, cost).

## 3. Public repo or private?

- Recommended: make it **public** with a clear license (MIT) once hygiene checks
  pass — most convincing for recruiters.
- Prerequisite: zero employer/internal traces (see the checklist below).
- If in doubt, keep it private and share by invite.

### High-quality public references (learn, respect licenses, do not copy wholesale)
1. **DataTalksClub/data-engineering-zoomcamp** — Kafka, Spark, Airflow, K8s.
2. **confluentinc/examples** (clickstream demo) — official Kafka streaming example.
3. **apache/flink-playgrounds** — official Flink playgrounds.
4. **aws-samples/eks-workshop** — official hands-on EKS walkthrough.

Write our own code so every line can be explained in interviews.

## 4. Checklist before making the repo public

- [ ] No employer code, configs, internal names, or internal metrics anywhere.
- [ ] No absolute personal paths or personal contact info in the repo.
- [ ] README clearly says "personal/portfolio project".
- [ ] LICENSE added (MIT recommended).
- [ ] Measured metrics are real and reproducible from our own runs.
