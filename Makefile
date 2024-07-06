.DEFAULT_GOAL := help

install: ## Install dependencies and `ruff` pre-commit hooks
	pre-commit install
	poetry install --with dev

test-local: ## Run tests locally
	poetry run pytest tests --cov=pgmq_sqlalchemy.queue

test-docker: ## Run tests in docker
	docker rmi -f pgmq-sqlalchemy-pgmq_tests
	docker build -t pgmq-sqlalchemy-pgmq_tests -f Dockerfile .
	docker compose run --rm pgmq_tests

clear-db: ## Clear the database
	docker compose down pgmq_postgres
	rm -r stateful_volumes/pgmq_postgres/

start-db: ## Start the database
	docker compose up -d pgmq_postgres

exec-db: ## Enter the database container
	docker compose exec pgmq_postgres psql -U postgres -d postgres

.PHONY: test-local test-docker clear-db start-db

# generate help from comments
.PHONY: help
help: ## Display this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'