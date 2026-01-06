import os

import pytest
from pytest import FixtureRequest
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, Session

from pgmq_sqlalchemy import PGMQueue
from tests.constant import ASYNC_DRIVERS, SYNC_DRIVERS

# Async fixture names for test filtering
ASYNC_FIXTURE_NAMES = [
    "pgmq_by_async_dsn",
    "pgmq_by_async_engine",
    "pgmq_by_async_session_maker",
]


def pytest_addoption(parser: pytest.Parser):
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


def pytest_generate_tests(metafunc: pytest.Metafunc):
    """
    Dynamically generate test parametrization based on CLI options.

    This allows us to parametrize fixtures based on the --driver option.
    """
    if "pgmq_all_variants" in metafunc.fixturenames:
        driver_from_cli = metafunc.config.getoption("--driver")

        # Define sync and async fixture variants
        sync_fixtures = [
            "pgmq_by_dsn",
            "pgmq_by_engine",
            "pgmq_by_session_maker",
            "pgmq_by_dsn_and_engine",
            "pgmq_by_dsn_and_session_maker",
        ]

        async_fixtures = [
            "pgmq_by_async_dsn",
            "pgmq_by_async_engine",
            "pgmq_by_async_session_maker",
            "pgmq_by_async_dsn_and_async_engine",
            "pgmq_by_async_dsn_and_async_session_maker",
        ]

        # Determine which fixtures to use
        if not driver_from_cli:
            # No driver specified, use all fixtures
            fixture_params = sync_fixtures + async_fixtures
        elif driver_from_cli in ASYNC_DRIVERS:
            # Async driver specified
            fixture_params = async_fixtures
        else:
            # Sync driver specified
            fixture_params = sync_fixtures

        # Parametrize the test
        metafunc.parametrize("pgmq_all_variants", fixture_params, indirect=True)


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
def get_sa_db(request: pytest.FixtureRequest):
    """Get database name from CLI argument or environment variable."""
    db_name_from_cli = request.config.getoption("--db-name")
    if db_name_from_cli:
        return db_name_from_cli
    return os.getenv("SQLALCHEMY_DB", "postgres")


@pytest.fixture(scope="function")
def get_dsn(
    request: FixtureRequest,
    get_sa_host,
    get_sa_port,
    get_sa_user,
    get_sa_password,
    get_sa_db,
):
    """Get DSN for sync drivers based on CLI option."""
    driver_from_cli = request.config.getoption("--driver")

    # Use CLI driver if specified and it's a sync driver
    if driver_from_cli and driver_from_cli in SYNC_DRIVERS:
        driver = driver_from_cli
    else:
        # Default to first sync driver if no CLI option or invalid
        driver = SYNC_DRIVERS[0]

    return f"postgresql+{driver}://{get_sa_user}:{get_sa_password}@{get_sa_host}:{get_sa_port}/{get_sa_db}"


@pytest.fixture(scope="function")
def get_async_dsn(
    request: FixtureRequest,
    get_sa_host,
    get_sa_port,
    get_sa_user,
    get_sa_password,
    get_sa_db,
):
    """Get DSN for async drivers based on CLI option."""
    driver_from_cli = request.config.getoption("--driver")

    # Use CLI driver if specified and it's an async driver
    if driver_from_cli and driver_from_cli in ASYNC_DRIVERS:
        driver = driver_from_cli
    else:
        # Default to first async driver if no CLI option or invalid
        driver = ASYNC_DRIVERS[0]

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
def pgmq_by_async_dsn_and_async_engine(get_async_dsn, get_async_engine):
    pgmq = PGMQueue(dsn=get_async_dsn, engine=get_async_engine)
    return pgmq


@pytest.fixture(scope="function")
def pgmq_by_dsn_and_session_maker(get_dsn, get_session_maker):
    pgmq = PGMQueue(dsn=get_dsn, session_maker=get_session_maker)
    return pgmq


@pytest.fixture(scope="function")
def pgmq_by_async_dsn_and_async_session_maker(get_async_dsn, get_async_session_maker):
    pgmq = PGMQueue(dsn=get_async_dsn, session_maker=get_async_session_maker)
    return pgmq


@pytest.fixture(scope="function")
def db_session(get_session_maker) -> "Session":
    with get_session_maker() as session:
        yield session


@pytest.fixture(scope="function")
async def async_db_session(get_async_session_maker) -> "AsyncSession":
    async with get_async_session_maker() as session:
        yield session
