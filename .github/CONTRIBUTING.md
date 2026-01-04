# Contributing

Welcome to contribute to `pgmq-sqlalchemy` !  <br>
This document will guide you through the process of contributing to the project.

## How to Contribute

1. Fork the repository
    - Click the `Fork` button in the upper right corner of the repository page.
2. Clone the repository
    - Clone the repository to your local machine.
    ```bash
    git clone https://github.com/your-username/pgmq-sqlalchemy.git
    ```
3. Create a new branch
    - Create a new branch for your changes.
    ```bash
    git checkout -b feature/your-feature-name
    ```
4. Make your changes
    - Make your changes to the codebase.
    - Add tests for your changes.
    - Add documentation if changes are user-facing.
5. Commit your changes
    - Commit your changes with meaningful commit messages.
        - [ref: conventional git commit messages](https://www.conventionalcommits.org/en/v1.0.0/)
    ```bash
    git commit -m "feat: your feature description"
    ```
6. Push your changes
    - Push your changes to your forked repository.
    ```bash
    git push origin feature/your-feature-name
    ```
7. Create a Pull Request
    - Create a Pull Request from your forked repository to the `develop` branch of the original repository.

## Development

### Setup

Install dependencies and `ruff` pre-commit hooks.
```bash
make install
```

> Prerequisites: **Docker** and **Docker Compose** installed.

Start development PostgreSQL
```bash
make start-db
```

Stop development PostgreSQL
```bash
make stop-db
```

### Makefile utility

```bash
make help
```
> will show all available commands and their descriptions.

### Linting 

We use [pre-commit](https://pre-commit.com/) hook with [ruff](https://github.com/astral-sh/ruff-pre-commit) to automatically lint the codebase before committing.


### Testing

#### Quick Start

Run tests locally with default settings:
```bash
make test-local
```

#### GitHub Actions-style Testing

To test in the same way as GitHub Actions, follow these steps:

1. Setup environment files (first time only):
```bash
make setup-env
```

2. Start the PostgreSQL database:
```bash
make start-db
```

3. Setup a test database with pgmq extension:
```bash
make setup-test-db DB_NAME=my_test_db
```

4. Run tests with a specific driver and database:
```bash
make test-with-driver DRIVER=psycopg2 DB_NAME=my_test_db
```

5. Teardown the test database after testing:
```bash
make teardown-test-db DB_NAME=my_test_db
```

Available drivers:
- Sync drivers: `pg8000`, `psycopg2`, `psycopg`, `psycopg2cffi`
- Async drivers: `asyncpg`

#### Run All Tests in Parallel

To run tests for all drivers in parallel (similar to CI matrix):
```bash
make test-all
```

This command will:
1. Set up test databases for all drivers
2. Run tests for all drivers in parallel
3. Clean up all test databases after completion

#### Alternative Testing Methods

Run tests for a specific driver (uses default database):
```bash
uv run pytest tests --driver=psycopg2
```

Run tests with a specific database name:
```bash
uv run pytest tests --driver=psycopg2 --db-name=custom_db
```

Run tests in docker:
```bash
make test-docker
```

### Documentation

Serve documentation
```bash
make doc-serve
```

Clean documentation build
```bash
make doc-clean
```