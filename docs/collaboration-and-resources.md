# Clickstream Project — Collaboration, Ethics & Resources

Status: v0.1 · Scope: decision notes for the resume-building clickstream project.

## 1. Asking an ex-teammate about the tech stack / inviting her to collaborate

**Overall: it is OK to do, with clear guardrails.** The main risk is accidentally carrying
Intuit-internal information into a public project.

### Safe to discuss (industry-standard, not confidential)
- Apache Kafka, Spark, Flink, AWS EMR / MSK / EKS, Prometheus, Grafana, Docker, Kubernetes, PostgreSQL, etc.

### Do NOT bring into this project (Intuit-confidential)
- Internal proprietary tool / platform names
- Internal architecture documents
- Exact production data volumes / business metrics
- Internal configuration or code snippets
- Any Intuit code or config files copied into this repo

### Collaboration guardrails
- If the teammate is **still employed at Intuit**: no company time, company laptop, or company accounts
  for this project; check their employment agreement for outside-work / moonlighting clauses.
- If the teammate is **ex-Intuit** (like you): NDA/confidentiality obligations usually persist —
  keep the project independent and generic anyway.
- Keep contributions visible and honest (GitHub commit history); each person's work should be
  attributable for resume purposes.
- Present it as a **collaborative portfolio project**, never as Intuit production experience.

## 2. Toy project vs. actual production project

- This repo is a **small, independent, from-scratch demo** built with public/standard technologies.
- Its purpose: gain hands-on experience with tech the team used but that we had not personally
  operated (e.g., Kafka, EKS, Flink).
- It is **completely different in scale and compute** from the real production system.
- **Resume rule:** always label it as a **personal / portfolio project**, with your own measured
  metrics (throughput, latency, lag, cost). Never imply it was Intuit production experience.

## 3. Public repo or private? Reference repos

### Recommendation: make it PUBLIC
- Public GitHub (with a clear license, e.g., MIT — like nanotrack) is the most convincing for recruiters.
- Prerequisite: ensure zero Intuit traces (see Section 1).
- If concerned, keep it private and share via invite — but public is more useful for job applications.

### High-quality public references (learn from them; respect their licenses; do not copy wholesale)
1. **DataTalksClub/data-engineering-zoomcamp** — comprehensive data-engineering course: Kafka, Spark,
   Airflow, containers/K8s; best overall structure to follow.
2. **confluentinc/examples** (clickstream demo) — official Kafka streaming example, closest to a
   clickstream scenario.
3. **apache/flink-playgrounds** — official Flink playgrounds; best way to close the Flink gap.
4. **aws-samples/eks-workshop** — official hands-on EKS deployment walkthrough.

### Suggested combination
Use the DE Zoomcamp structure + Confluent clickstream ideas, but **write our own code**
(see DESIGN.md) so every line can be explained in interviews.

## 4. Quick checklist before making the repo public

- [ ] No Intuit code, configs, internal names, or internal metrics anywhere in the repo
- [ ] README clearly says "personal/portfolio project"
- [ ] LICENSE added (MIT recommended)
- [ ] No personal contact info beyond what you want public
- [ ] Measured metrics are real and reproducible from your own runs
