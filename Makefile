test-local:
	poetry run pytest tests

test-docker:
	docker rmi -f pgmq-sqlalchemy-pgmq_tests
	docker build -t pgmq-sqlalchemy-pgmq_tests -f Dockerfile .
	docker compose run --rm pgmq_tests

clear-db:
	docker compose down pgmq_postgres
	rm -r stateful_volumes/pgmq_postgres/

start-db:
	docker compose up -d pgmq_postgres

.PHONY: test-local test-docker clear-db start-db