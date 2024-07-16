# Contributing

Welcome to contribute to `pgmq-sqlalchemy` ! 
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

Run tests in local
```bash
make test-local
```

Run tests in docker
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