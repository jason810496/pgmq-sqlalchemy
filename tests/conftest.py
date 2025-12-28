import os

import pytest
from pytest import FixtureRequest
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, Session

from pgmq_sqlalchemy import PGMQueue
from tests.constant import ASYNC_DRIVERS, SYNC_DRIVERS

# Async fixture names for test filtering
ASYNC_FIXTURE_NAMES = ['pgmq_by_async_dsn', 'pgmq_by_async_engine', 'pgmq_by_async_session_maker']


def pytest_addoption(parser):
    """Add custom command-line options for pytest."""
    parser.addoption(
        "--driver",
        action="store",
        default=None,
        help="Specify the database driver to use for testing (e.g., psycopg2, asyncpg, pg8000, etc.)",
    )
    parser.addoption(
        "--db-name",
        action="store",
        default=None,
        help="Specify the database name to use for testing",
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection to skip tests not matching the --driver option."""
    driver_from_cli = config.getoption("--driver")

    if not driver_from_cli:
        # No driver specified, run all tests
        return

    # Determine if the specified driver is sync or async
    is_async_driver = driver_from_cli in ASYNC_DRIVERS
    is_sync_driver = driver_from_cli in SYNC_DRIVERS

    if not is_async_driver and not is_sync_driver:
        # Invalid driver
        return

    # Filter out tests that don't match the specified driver
    skip_marker = pytest.mark.skip(reason=f"Test uses different driver (--driver={driver_from_cli} specified)")

    for item in items:
        # Parse the test name to extract driver info
        # Format is usually: test_name[fixture_name-driver_name]
        item_id = item.nodeid

        # Check if the test has a specific driver in its ID
        # Extract driver name from test ID (e.g., test_name[pgmq_by_dsn-psycopg2])
        if '[' in item_id and ']' in item_id:
            # Extract the part between brackets
            bracket_content = item_id[item_id.find('[')+1:item_id.find(']')]

            # Check for async fixtures by name (more precise than string matching)
            is_async_test = any(async_fixture in bracket_content for async_fixture in ASYNC_FIXTURE_NAMES)

            # Skip async tests if sync driver specified
            if is_sync_driver and is_async_test:
                item.add_marker(skip_marker)
                continue

            # Skip sync tests if async driver specified
            if is_async_driver and not is_async_test:
                item.add_marker(skip_marker)
                continue

            # Check if any known driver is in the bracket content
            # Sort drivers by length (descending) to match longer names first (e.g., psycopg2cffi before psycopg2)
            sorted_drivers = sorted(SYNC_DRIVERS + ASYNC_DRIVERS, key=len, reverse=True)
            for driver in sorted_drivers:
                if f"-{driver}]" in item_id or f"-{driver}-" in bracket_content:
                    # This test is for a specific driver
                    if driver != driver_from_cli:
                        # Skip if it doesn't match the CLI driver
                        item.add_marker(skip_marker)
                    break


@pytest.fixture(scope="module")
def get_sa_host():
    return os.getenv("SQLALCHEMY_HOST", "localhost")


@pytest.fixture(scope="module")
def get_sa_port():
    return os.getenv("SQLALCHEMY_PORT", "5432")


@pytest.fixture(scope="module")
def get_sa_user():
    return os.getenv("SQLALCHEMY_USER", "postgres")


@pytest.fixture(scope="module")
def get_sa_password():
    return os.getenv("SQLALCHEMY_PASSWORD", "postgres")


@pytest.fixture(scope="module")
def get_sa_db(request):
    """Get database name from CLI argument or environment variable."""
    db_name_from_cli = request.config.getoption("--db-name")
    if db_name_from_cli:
        return db_name_from_cli
    return os.getenv("SQLALCHEMY_DB", "postgres")


@pytest.fixture(scope="function", params=SYNC_DRIVERS)
def get_dsn(
    request: FixtureRequest,
    get_sa_host,
    get_sa_port,
    get_sa_user,
    get_sa_password,
    get_sa_db,
):
    """Get DSN for sync drivers."""
    driver_from_cli = request.config.getoption("--driver")
    
    # Use CLI driver if specified, otherwise use parametrized driver
    if driver_from_cli and driver_from_cli in SYNC_DRIVERS:
        driver = driver_from_cli
    else:
        driver = request.param
    
    return f"postgresql+{driver}://{get_sa_user}:{get_sa_password}@{get_sa_host}:{get_sa_port}/{get_sa_db}"


@pytest.fixture(scope="function", params=ASYNC_DRIVERS)
def get_async_dsn(
    request: FixtureRequest,
    get_sa_host,
    get_sa_port,
    get_sa_user,
    get_sa_password,
    get_sa_db,
):
    """Get DSN for async drivers."""
    driver_from_cli = request.config.getoption("--driver")
    
    # Use CLI driver if specified, otherwise use parametrized driver
    if driver_from_cli and driver_from_cli in ASYNC_DRIVERS:
        driver = driver_from_cli
    else:
        driver = request.param
    
    return f"postgresql+{driver}://{get_sa_user}:{get_sa_password}@{get_sa_host}:{get_sa_port}/{get_sa_db}"


@pytest.fixture(scope="function")
def get_engine(get_dsn):
    return create_engine(get_dsn)


@pytest.fixture(scope="function")
def get_async_engine(get_async_dsn):
    return create_async_engine(get_async_dsn)


@pytest.fixture(scope="function")
def get_session_maker(get_engine):
    return sessionmaker(bind=get_engine, class_=Session)


@pytest.fixture(scope="function")
def get_async_session_maker(get_async_engine):
    return sessionmaker(bind=get_async_engine, class_=AsyncSession)


@pytest.fixture(scope="function")
def pgmq_by_dsn(get_dsn):
    pgmq = PGMQueue(dsn=get_dsn)
    return pgmq


@pytest.fixture(scope="function")
def pgmq_by_async_dsn(get_async_dsn):
    pgmq = PGMQueue(dsn=get_async_dsn)
    return pgmq


@pytest.fixture(scope="function")
def pgmq_by_engine(get_engine):
    pgmq = PGMQueue(engine=get_engine)
    return pgmq


@pytest.fixture(scope="function")
def pgmq_by_async_engine(get_async_engine):
    pgmq = PGMQueue(engine=get_async_engine)
    return pgmq


@pytest.fixture(scope="function")
def pgmq_by_session_maker(get_session_maker):
    pgmq = PGMQueue(session_maker=get_session_maker)
    return pgmq


@pytest.fixture(scope="function")
def pgmq_by_async_session_maker(get_async_session_maker):
    pgmq = PGMQueue(session_maker=get_async_session_maker)
    return pgmq


@pytest.fixture(scope="function")
def pgmq_by_dsn_and_engine(get_dsn, get_engine):
    pgmq = PGMQueue(dsn=get_dsn, engine=get_engine)
    return pgmq


@pytest.fixture(scope="function")
def pgmq_by_dsn_and_session_maker(get_dsn, get_session_maker):
    pgmq = PGMQueue(dsn=get_dsn, session_maker=get_session_maker)
    return pgmq


@pytest.fixture(scope="function")
def db_session(get_session_maker) -> Session:
    return get_session_maker()
