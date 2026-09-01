.PHONY: up down ps logs test lint kind-up kind-down eks-apply eks-destroy

up:
	docker compose up -d --build

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f

test:
	pytest tests/ -v

lint:
	ruff check producer spark_jobs etl api tests

kind-up:
	kind create cluster --name clickstream

kind-down:
	kind delete cluster --name clickstream

eks-apply:
	cd terraform && terraform init && terraform apply

eks-destroy:
	cd terraform && terraform destroy
