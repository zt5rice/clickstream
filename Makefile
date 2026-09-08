# clickstream - local development tooling
.PHONY: init up down demo ps logs test test-ci lint fmt dbt-run dbt-test delta-demo quality-run go-ops-test go-ops-build ansible-check ansible-provision ai-assistant-up kind-up kind-down kind-load kind-apply kind-logs kind-topics kind-spark-check helm-lint helm-template helm-up helm-install-cert-manager eks-init eks-validate eks-plan eks-plan-destroy eks-apply eks-destroy eks-destroy-all load-test check

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

ansible-check: ## Run the local-demo Ansible playbook in --check mode
	.venv/bin/ansible-playbook -i ansible/inventory.yml ansible/playbooks/provision-local-demo.yml --check

ansible-provision: ## Run the local-demo Ansible playbook (starts local services)
	.venv/bin/ansible-playbook -i ansible/inventory.yml ansible/playbooks/provision-local-demo.yml

ai-assistant-up: ## Start the AI assistant service (mock LLM by default)
	docker compose up -d ai-assistant

check: ## Lint, format-check, and run the test suite
	$(RUFF) format --check producer spark_jobs etl api quality tests
	$(RUFF) check producer spark_jobs etl api quality tests
	$(PYTEST) tests/ -v

kind-up:
	kind create cluster --config k8s/kind-config.yaml --name clickstream

kind-down:
	kind delete cluster --name clickstream

kind-load: ## Load locally-built API image into kind
	kind load docker-image clickstream-api:k8s-20260906 --name clickstream
	kind load docker-image clickstream-spark-submit:k8s-20260906 --name clickstream

kind-apply: ## Apply the clickstream k8s manifests (core subset)
	kubectl apply -k k8s/

kind-logs: ## Tail logs of kind workloads in the clickstream namespace
	kubectl logs -n clickstream -l app=api --tail=50

kind-topics: ## Create Kafka topics via the kafka-init Job (idempotent)
	kubectl apply -f k8s/kafka-init.yaml -n clickstream

kind-spark-check: ## Run the Spark (Delta) verification Job
	kubectl apply -f k8s/spark-check-job.yaml -n clickstream

helm-lint: ## Lint the clickstream Helm chart
	helm lint k8s/helm/clickstream

helm-template: ## Render the chart with helm template
	helm template clickstream k8s/helm/clickstream

helm-up: ## Install/upgrade the chart into the clickstream namespace
	helm upgrade --install clickstream k8s/helm/clickstream \
		--namespace clickstream --create-namespace

helm-install-cert-manager: ## Install cert-manager v1.16.3 (self-signed issuer)
	kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.16.3/cert-manager.yaml

LOAD_TEST_IMAGE ?= grafana/k6:0.54.0

load-test: ## Run the k6 read-API load test (needs the local stack up)
	docker run --rm --add-host host.docker.internal:host-gateway \
		-v "$(PWD)/scripts/load/k6:/scripts" \
		$(LOAD_TEST_IMAGE) run /scripts/read-api.js \
		-e BASE_URL=http://host.docker.internal:8000

eks-init: ## Initialize Terraform (downloads providers/modules)
	cd terraform && terraform init

eks-validate: ## Validate Terraform configuration
	cd terraform && terraform validate

eks-plan: ## Terraform plan (requires AWS credentials; no changes applied)
	cd terraform && terraform plan

eks-plan-destroy: ## Show what terraform destroy would remove (no changes)
	cd terraform && terraform plan -destroy

eks-apply:
	cd terraform && terraform init && terraform validate && terraform apply

eks-destroy: ## DESTROY the EKS stack (auto-approve) - run after every demo!
	@echo "WARNING: destroying all EKS resources created by terraform/"
	cd terraform && terraform destroy -auto-approve

eks-destroy-all: ## Teardown EKS + local kind cluster + compose (full cleanup)
	$(MAKE) eks-destroy
	$(MAKE) kind-down
