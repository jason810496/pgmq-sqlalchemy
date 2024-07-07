import uuid
import pytest

from sqlalchemy.exc import ProgrammingError
from pgmq_sqlalchemy import PGMQueue

from tests.fixture_deps import (
    pgmq_deps,
    PGMQ_WITH_QUEUE,
    pgmq_setup_teardown,
    pgmq_partitioned_setup_teardown,
)

from tests._utils import check_queue_exists

use_fixtures = [
    pgmq_setup_teardown,
    pgmq_partitioned_setup_teardown,
]


@pgmq_deps
def test_create_queue(pgmq_fixture, db_session):
    pgmq: PGMQueue = pgmq_fixture
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    pgmq.create_queue(queue_name)
    assert check_queue_exists(db_session, queue_name) is True


@pgmq_deps
def test_create_partitioned_queue(pgmq_fixture, db_session):
    pgmq: PGMQueue = pgmq_fixture
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    pgmq.create_partitioned_queue(queue_name)
    assert check_queue_exists(db_session, queue_name) is True


def test_create_same_queue(pgmq_setup_teardown: PGMQ_WITH_QUEUE, db_session):
    pgmq, queue_name = pgmq_setup_teardown
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    pgmq.create_queue(queue_name)
    assert check_queue_exists(db_session, queue_name) is True
    pgmq.create_queue(queue_name)
    # `create_queue` with the same queue name should not raise an exception
    # and the queue should still exist
    assert check_queue_exists(db_session, queue_name) is True


@pgmq_deps
def test_validate_queue_name(pgmq_fixture):
    pgmq: PGMQueue = pgmq_fixture
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    pgmq.validate_queue_name(queue_name)
    # `queue_name` should be a less than 48 characters
    with pytest.raises(Exception) as e:
        pgmq.validate_queue_name("a" * 49)
    error_msg: str = str(e.value.orig)
    assert "queue name is too long, maximum length is 48 characters" in error_msg


def test_drop_queue(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    _ = pgmq_setup_teardown
    pass


@pgmq_deps
def test_drop_non_exist_queue(pgmq_fixture, db_session):
    pgmq: PGMQueue = pgmq_fixture
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    assert check_queue_exists(db_session, queue_name) is False
    with pytest.raises(ProgrammingError):
        pgmq.drop_queue(queue_name)


def test_drop_partitioned_queue(pgmq_partitioned_setup_teardown: PGMQ_WITH_QUEUE):
    _ = pgmq_partitioned_setup_teardown
    pass


@pgmq_deps
def test_drop_non_exist_partitioned_queue(pgmq_fixture, db_session):
    pgmq: PGMQueue = pgmq_fixture
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    assert check_queue_exists(db_session, queue_name) is False
    with pytest.raises(ProgrammingError):
        pgmq.drop_queue(queue_name, partitioned=True)
