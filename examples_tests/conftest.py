"""Pytest configuration for examples tests."""
import os
import pytest

import logging


def pytest_addoption(parser):
    """Add custom command-line options for pytest."""
    parser.addoption(
        "--db-name",
        action="store",
        default=None,
        help="Specify the database name to use for testing",
    )

@pytest.fixture(scope="module")
def configure_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s][%(asctime)s][%(name)s] %(message)s"
    )


@pytest.fixture(scope="module")
def examples_dir():
    """Return the path to the examples directory."""
    return os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "examples",
        "fastapi_pub_sub"
    )


@pytest.fixture(scope="module")
def database_url(request):
    """Get database URL from environment or CLI."""
    db_name = request.config.getoption("--db-name")
    if not db_name:
        db_name = os.getenv("SQLALCHEMY_DB", "postgres")
    
    host = os.getenv("SQLALCHEMY_HOST", "localhost")
    port = os.getenv("SQLALCHEMY_PORT", "5432")
    user = os.getenv("SQLALCHEMY_USER", "postgres")
    password = os.getenv("SQLALCHEMY_PASSWORD", "postgres")
    
    return f"postgresql://{user}:{password}@{host}:{port}/{db_name}"


@pytest.fixture(scope="module")
def sync_database_url(database_url):
    """Get sync database URL with psycopg2 driver."""
    return database_url.replace("postgresql://", "postgresql+psycopg2://")


@pytest.fixture(scope="module")
def async_database_url(database_url):
    """Get async database URL with asyncpg driver."""
    return database_url.replace("postgresql://", "postgresql+asyncpg://")
