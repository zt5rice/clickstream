# clickstream - local development tooling
.PHONY: init up down demo ps logs test test-ci lint fmt dbt-run dbt-test delta-demo quality-run go-ops-test go-ops-build check kind-up kind-down eks-apply eks-destroy

# Prefer the pinned tools inside .venv when present (after `make init`).
RUFF ?= $(if $(wildcard .venv/bin/ruff),.venv/bin/ruff,ruff)
PYTEST ?= $(if $(wildcard .venv/bin/pytest),.venv/bin/pytest,pytest)

init: ## Create .venv and install pinned dev dependencies
	python3 -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -r requirements-dev.txt

up:
	docker compose up -d --build

demo: ## Start the full stack in the foreground
	docker compose up --build

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f

test:
	$(PYTEST) tests/ -v

test-ci: ## Run the fast test set (excludes slow-marked tests) for CI
	$(PYTEST) tests/ -m "not slow and not integration" -v

lint:
	$(RUFF) check producer spark_jobs etl api quality tests

fmt:
	$(RUFF) format producer spark_jobs etl api quality tests
	$(RUFF) check --fix producer spark_jobs etl api quality tests

dbt-run: ## Run dbt models against the local curated Postgres
	.venv/bin/dbt run --project-dir dbt --profiles-dir dbt

dbt-test: ## Run dbt tests against the local curated Postgres
	.venv/bin/dbt test --project-dir dbt --profiles-dir dbt

delta-demo: ## Run the local Delta Lake demo (needs spark-master/spark-worker up)
	docker compose up -d spark-master spark-worker
	docker compose run --rm --no-deps spark-submit \
		/opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--packages io.delta:delta-spark_2.12:3.2.1 \
		--conf spark.jars.ivy=/tmp/.ivy2 \
		/opt/clickstream/spark_jobs/delta_lakehouse.py all

quality-run: ## Run Soda (Postgres) + native ClickHouse checks and write reports
	.venv/bin/python quality/run_quality.py

GO_IMAGE ?= golang:1.24.3-alpine

go-ops-test: ## Run go_ops unit tests in a pinned golang container
	docker run --rm -v "$(PWD)/go_ops:/app" -w /app $(GO_IMAGE) go test ./...

go-ops-build: ## Build the go_ops CLI into go_ops/bin/
	docker run --rm -v "$(PWD)/go_ops:/app" -w /app $(GO_IMAGE) go build -o /app/bin/go_ops .

check: ## Lint, format-check, and run the test suite
	$(RUFF) format --check producer spark_jobs etl api quality tests
	$(RUFF) check producer spark_jobs etl api quality tests
	$(PYTEST) tests/ -v

kind-up:
	kind create cluster --name clickstream

kind-down:
	kind delete cluster --name clickstream

eks-apply:
	cd terraform && terraform init && terraform apply

eks-destroy:
	cd terraform && terraform destroy
