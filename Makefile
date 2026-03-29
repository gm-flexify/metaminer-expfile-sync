.PHONY: help build up down logs restart clean test shell shell-db migrate migrate-create

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

build: ## Build Docker images
	docker compose build

up: ## Start all services
	docker compose up -d

down: ## Stop all services
	docker compose down

logs: ## Follow app logs
	docker compose logs -f app

logs-db: ## Follow DB logs
	docker compose logs -f db

restart: ## Restart all services
	docker compose restart

clean: ## Stop and remove volumes
	docker compose down -v

test: ## Run tests
	docker compose run --rm app pytest

shell: ## Open shell in app container
	docker compose exec app bash

shell-db: ## Open psql in DB container
	docker compose exec db psql -U $${POSTGRES_USER:-expfile_user} -d $${POSTGRES_DB:-expfile_sync}

migrate: ## Run alembic upgrade head
	docker compose run --rm app alembic upgrade head

migrate-create: ## Create new migration (usage: make migrate-create msg="description")
	docker compose run --rm app alembic revision --autogenerate -m "$(msg)"

migrate-history: ## Show migration history
	docker compose run --rm app alembic history
