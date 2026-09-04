# clickstream - local development tooling
.PHONY: init up down demo ps logs test test-ci lint fmt dbt-run dbt-test delta-demo check kind-up kind-down eks-apply eks-destroy

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
	$(RUFF) check producer spark_jobs etl api tests

fmt:
	$(RUFF) format producer spark_jobs etl api tests
	$(RUFF) check --fix producer spark_jobs etl api tests

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

check: ## Lint, format-check, and run the test suite
	$(RUFF) format --check producer spark_jobs etl api tests
	$(RUFF) check producer spark_jobs etl api tests
	$(PYTEST) tests/ -v

kind-up:
	kind create cluster --name clickstream

kind-down:
	kind delete cluster --name clickstream

eks-apply:
	cd terraform && terraform init && terraform apply

eks-destroy:
	cd terraform && terraform destroy
