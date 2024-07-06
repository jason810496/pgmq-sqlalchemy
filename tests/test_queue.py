import uuid

from pgmq_sqlalchemy import PGMQueue

from tests.fixture_deps import (
    pgmq_deps,
    pgmq_setup_teardown,
)

from tests._utils import check_queue_exists

use_fixtures = [
    pgmq_setup_teardown,
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
