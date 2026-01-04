.DEFAULT_GOAL := help

# Test drivers configuration
TEST_DRIVERS := pg8000 psycopg2 psycopg psycopg2cffi asyncpg

install: ## Install dependencies and `ruff` pre-commit hooks
	pre-commit install
	uv sync --extra dev

build: ## Build the package
	uv build

setup-env: ## Copy template environment files
	cp pgmq_postgres.template.env pgmq_postgres.env
	cp pgmq_tests.template.env pgmq_tests.env

test-local: ## Run tests locally
	uv run pytest tests --cov=pgmq_sqlalchemy.queue


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

setup-test-db: ## Setup test database with pgmq extension (Usage: make setup-test-db DB_NAME=test_db)
ifndef DB_NAME
	@echo "Error: DB_NAME is required. Usage: make setup-test-db DB_NAME=test_db"
	@exit 1
endif
	docker compose exec -T pgmq_postgres psql -U postgres -c "DROP DATABASE IF EXISTS $(DB_NAME);" || true
	docker compose exec -T pgmq_postgres psql -U postgres -c "CREATE DATABASE $(DB_NAME);"
	docker compose exec -T pgmq_postgres psql -U postgres -d $(DB_NAME) -c "CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;"

teardown-test-db: ## Teardown test database (Usage: make teardown-test-db DB_NAME=test_db)
ifndef DB_NAME
	@echo "Error: DB_NAME is required. Usage: make teardown-test-db DB_NAME=test_db"
	@exit 1
endif
	docker compose exec -T pgmq_postgres psql -U postgres -c "DROP DATABASE IF EXISTS $(DB_NAME);" || true

test-with-driver: ## Run tests with specific driver and database (Usage: make test-with-driver DRIVER=psycopg2 DB_NAME=test_db)
ifndef DRIVER
	@echo "Error: DRIVER is required."
	@echo "Supported drivers: pg8000, psycopg2, psycopg, psycopg2cffi, asyncpg"
	@echo "Usage: make test-with-driver DRIVER=psycopg2 DB_NAME=test_db"
	@exit 1
endif
ifndef DB_NAME
	@echo "Error: DB_NAME is required. Usage: make test-with-driver DRIVER=psycopg2 DB_NAME=test_db"
	@exit 1
endif
	@echo "Validating database existence..."
	@if ! docker compose exec -T pgmq_postgres psql -U postgres -lqt | cut -d \| -f 1 | grep -qw $(DB_NAME); then \
		echo "Error: Database '$(DB_NAME)' does not exist."; \
		echo "Available databases:"; \
		docker compose exec -T pgmq_postgres psql -U postgres -lqt | cut -d \| -f 1 | grep -v '^$$' | grep -v '^ *$$' | sed 's/^/  - /'; \
		echo ""; \
		echo "Create the database first with: make setup-test-db DB_NAME=$(DB_NAME)"; \
		exit 1; \
	fi
	@echo "Running tests with driver=$(DRIVER) on database=$(DB_NAME)..."
	uv run pytest tests --driver=$(DRIVER) --db-name=$(DB_NAME) --cov=pgmq_sqlalchemy.queue

test-all: ## Run tests for all drivers in parallel (Usage: make test-all)
	@echo "Running tests for all drivers in parallel..."
	@echo "Setting up databases..."
	@$(MAKE) setup-test-db DB_NAME=test_pg8000
	@$(MAKE) setup-test-db DB_NAME=test_psycopg2
	@$(MAKE) setup-test-db DB_NAME=test_psycopg
	@$(MAKE) setup-test-db DB_NAME=test_psycopg2cffi
	@$(MAKE) setup-test-db DB_NAME=test_asyncpg
	@echo "Running tests in parallel..."
	@$(MAKE) -j5 test-pg8000 test-psycopg2 test-psycopg test-psycopg2cffi test-asyncpg || \
		(echo "Some tests failed, cleaning up..." && $(MAKE) teardown-all-test-dbs && exit 1)
	@echo "All tests passed! Cleaning up..."
	@$(MAKE) teardown-all-test-dbs
	@echo "Done!"

test-pg8000:
	@echo "[pg8000] Running tests..."
	@uv run pytest tests --driver=pg8000 --db-name=test_pg8000 --cov=pgmq_sqlalchemy.queue --cov-report=xml:coverage-pg8000.xml -q

test-psycopg2:
	@echo "[psycopg2] Running tests..."
	@uv run pytest tests --driver=psycopg2 --db-name=test_psycopg2 --cov=pgmq_sqlalchemy.queue --cov-report=xml:coverage-psycopg2.xml -q

test-psycopg:
	@echo "[psycopg] Running tests..."
	@uv run pytest tests --driver=psycopg --db-name=test_psycopg --cov=pgmq_sqlalchemy.queue --cov-report=xml:coverage-psycopg.xml -q

test-psycopg2cffi:
	@echo "[psycopg2cffi] Running tests..."
	@uv run pytest tests --driver=psycopg2cffi --db-name=test_psycopg2cffi --cov=pgmq_sqlalchemy.queue --cov-report=xml:coverage-psycopg2cffi.xml -q

test-asyncpg:
	@echo "[asyncpg] Running tests..."
	@uv run pytest tests --driver=asyncpg --db-name=test_asyncpg --cov=pgmq_sqlalchemy.queue --cov-report=xml:coverage-asyncpg.xml -q

teardown-all-test-dbs:
	@echo "Cleaning up all test databases..."
	@$(MAKE) teardown-test-db DB_NAME=test_pg8000 || true
	@$(MAKE) teardown-test-db DB_NAME=test_psycopg2 || true
	@$(MAKE) teardown-test-db DB_NAME=test_psycopg || true
	@$(MAKE) teardown-test-db DB_NAME=test_psycopg2cffi || true
	@$(MAKE) teardown-test-db DB_NAME=test_asyncpg || true

clear-db: ## Clear the database
	docker compose down pgmq_postgres
	rm -r stateful_volumes/pgmq_postgres/

start-db: ## Start the database
	docker compose up -d pgmq_postgres
	while ! docker compose exec pgmq_postgres pg_isready; do sleep 1; done

exec-db: ## Enter the database container (Usage: make exec-db [DB_NAME=test_db])
ifdef DB_NAME
	docker compose exec pgmq_postgres psql -U postgres -d $(DB_NAME)
else
	docker compose exec pgmq_postgres psql -U postgres -d postgres
endif

doc-build: ## Build the documentation
	cd doc && uv run sphinx-build -nW . _build

doc-serve: doc-clean ## Serve the documentation
	cd doc && uv run sphinx-autobuild -nW . _build

doc-clean: ## Clean the documentation
	cd doc && rm -r _build

.PHONY: install build setup-env test-local test-with-driver test-all test-pg8000 test-psycopg2 test-psycopg test-psycopg2cffi test-asyncpg teardown-all-test-dbs setup-test-db teardown-test-db test-docker test-docker-rebuild clear-db start-db exec-db doc-build doc-serve doc-clean

# generate help from comments
.PHONY: help
help: ## Display this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'