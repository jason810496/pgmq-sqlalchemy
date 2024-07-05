test-local:
	poetry run pytest tests

test-docker:
	docker compose run --rm pgmq_tests

clear-db:
	docker compose down pgmq_postgres
	rm -r stateful_volumes/pgmq_postgres/

start-db:
	docker compose up -d pgmq_postgres