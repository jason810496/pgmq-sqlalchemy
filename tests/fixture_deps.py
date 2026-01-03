import uuid
from typing import Tuple

import pytest

from pgmq_sqlalchemy import PGMQueue
from tests._utils import check_queue_exists

PGMQ_WITH_QUEUE = Tuple[PGMQueue, str]


@pytest.fixture(scope="function")
def pgmq_all_variants(request: pytest.FixtureRequest) -> PGMQueue:
    """
    Fixture that parametrizes tests across all appropriate PGMQueue initialization methods.
    
    When --driver is specified, only fixtures matching that driver type (sync/async) are used.
    Without --driver, all fixtures are used.
    
    The parametrization is handled by pytest_generate_tests in conftest.py.
    
    Usage:
        def test_something(pgmq_all_variants):
            pgmq: PGMQueue = pgmq_all_variants
            # test code here
    """
    # The param is set by pytest_generate_tests via indirect parametrization
    return request.getfixturevalue(request.param)


@pytest.fixture(scope="function")
def pgmq_setup_teardown(pgmq_all_variants: PGMQueue, db_session) -> PGMQ_WITH_QUEUE:
    """
    Fixture that provides a PGMQueue instance with a unique temporary queue with setup and teardown.

    Args:
        pgmq_all_variants (PGMQueue): The PGMQueue instance (parametrized across all variants).
        db_session (sqlalchemy.orm.Session): The SQLAlchemy session object.

    Yields:
        tuple[PGMQueue,str]: A tuple containing the PGMQueue instance and the name of the temporary queue.

    Usage:
        def test_something(pgmq_setup_teardown):
            pgmq, queue_name = pgmq_setup_teardown
            # test code here

    """
    pgmq = pgmq_all_variants
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    assert check_queue_exists(db_session, queue_name) is False
    pgmq.create_queue(queue_name)
    assert check_queue_exists(db_session, queue_name) is True
    yield pgmq, queue_name
    pgmq.drop_queue(queue_name)
    assert check_queue_exists(db_session, queue_name) is False


@pytest.fixture(scope="function")
def pgmq_partitioned_setup_teardown(
    pgmq_all_variants: PGMQueue, db_session
) -> PGMQ_WITH_QUEUE:
    """
    Fixture that provides a PGMQueue instance with a unique temporary partitioned queue with setup and teardown.

    Args:
        pgmq_all_variants (PGMQueue): The PGMQueue instance (parametrized across all variants).
        db_session (sqlalchemy.orm.Session): The SQLAlchemy session object.

    Yields:
        tuple[PGMQueue,str]: A tuple containing the PGMQueue instance and the name of the temporary queue.

    Usage:
        def test_something(pgmq_partitioned_setup_teardown):
            pgmq, queue_name = pgmq_partitioned_setup_teardown
            # test code here

    """
    pgmq: PGMQueue = pgmq_all_variants
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    assert check_queue_exists(db_session, queue_name) is False
    pgmq.create_partitioned_queue(queue_name)
    assert check_queue_exists(db_session, queue_name) is True
    yield pgmq, queue_name
    pgmq.drop_queue(queue_name, partitioned=True)
    assert check_queue_exists(db_session, queue_name) is False
