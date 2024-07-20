.DEFAULT_GOAL := help

install: ## Install dependencies and `ruff` pre-commit hooks
	pre-commit install
	poetry install --with dev

build: ## Build the package
	poetry build

test-local: ## Run tests locally
	poetry run pytest tests --cov=pgmq_sqlalchemy.queue


test-docker-rebuild: ## Rebuild the docker image
	docker rmi -f pgmq-sqlalchemy-pgmq_tests
	docker build -t pgmq-sqlalchemy-pgmq_tests -f Dockerfile .

test-docker: test-docker-rebuild ## Run tests in docker
ifndef CMD
	if [ -d "stateful_volumes/htmlcov" ]; then rm -r stateful_volumes/htmlcov; fi
	if [ -d "htmlcov" ]; then rm -r htmlcov; fi
	docker compose run --rm pgmq_tests
	cp -r stateful_volumes/htmlcov/ htmlcov/
	rm -r stateful_volumes/htmlcov/
else
	docker run --rm --entrypoint '/bin/bash' pgmq-sqlalchemy-pgmq_tests -c '$(CMD)'
endif

clear-db: ## Clear the database
	docker compose down pgmq_postgres
	rm -r stateful_volumes/pgmq_postgres/

start-db: ## Start the database
	docker compose up -d pgmq_postgres
	while ! docker compose exec pgmq_postgres pg_isready; do sleep 1; done

exec-db: ## Enter the database container
	docker compose exec pgmq_postgres psql -U postgres -d postgres

doc-build: ## Build the documentation
	cd doc && poetry run sphinx-build -nW . _build

doc-serve: ## Serve the documentation
	cd doc && poetry run sphinx-autobuild -nW . _build

doc-clean: ## Clean the documentation
	cd doc && rm -r _build

.PHONY: install test-local test-docker test-docker-rebuild clear-db start-db exec-db doc-build doc-serve

# generate help from comments
.PHONY: help
help: ## Display this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'