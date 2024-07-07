import uuid
import pytest
import time

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


def test_list_queues(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    queues = pgmq.list_queues()
    assert queue_name in queues


def test_list_partitioned_queues(pgmq_partitioned_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_partitioned_setup_teardown
    queues = pgmq.list_queues()
    assert queue_name in queues


def test_send_and_read_msg(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    msg = {
        "foo": "bar",
        "hello": "world",
    }
    msg_id: int = pgmq.send(queue_name, msg)
    msg_read = pgmq.read(queue_name)
    assert msg_read.message == msg
    assert msg_read.msg_id == msg_id


def test_send_and_read_msg_with_delay(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    msg = {
        "foo": "bar",
        "hello": "world",
    }
    msg_id: int = pgmq.send(queue_name, msg, delay=2)
    msg_read = pgmq.read(queue_name)
    assert msg_read is None
    time.sleep(1)
    msg_read = pgmq.read(queue_name)
    assert msg_read is None
    time.sleep(1.1)
    msg_read = pgmq.read(queue_name)
    assert msg_read.message == msg
    assert msg_read.msg_id == msg_id


def test_send_and_read_msg_with_vt(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    msg = {
        "foo": "bar",
        "hello": "world",
    }
    msg_id: int = pgmq.send(queue_name, msg)
    msg_read = pgmq.read(queue_name, vt=2)
    assert msg_read.message == msg
    assert msg_read.msg_id == msg_id
    time.sleep(1.5)
    msg_read = pgmq.read(queue_name)
    assert msg_read is None
    time.sleep(0.6)
    msg_read = pgmq.read(queue_name)
    assert msg_read.message == msg
    assert msg_read.msg_id == msg_id


def test_send_and_read_msg_with_vt_and_delay(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    msg = {
        "foo": "bar",
        "hello": "world",
    }
    msg_id: int = pgmq.send(queue_name, msg, delay=2)
    msg_read = pgmq.read(queue_name, vt=2)
    assert msg_read is None
    time.sleep(1)
    msg_read = pgmq.read(queue_name, vt=2)
    assert msg_read is None
    time.sleep(1.1)
    msg_read = pgmq.read(queue_name, vt=2)
    assert msg_read.message == msg
    assert msg_read.msg_id == msg_id
    time.sleep(1.5)
    msg_read = pgmq.read(queue_name)
    assert msg_read is None
    time.sleep(0.6)
    msg_read = pgmq.read(queue_name)
    assert msg_read.message == msg
    assert msg_read.msg_id == msg_id


def test_read_empty_queue(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    msg_read = pgmq.read(queue_name)
    assert msg_read is None


def test_read_batch(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    msg = {
        "foo": "bar",
        "hello": "world",
    }
    msg_id_1: int = pgmq.send(queue_name, msg)
    msg_id_2: int = pgmq.send(queue_name, msg)
    msg_read = pgmq.read_batch(queue_name, 3)
    assert len(msg_read) == 2
    assert msg_read[0].message == msg
    assert msg_read[0].msg_id == msg_id_1
    assert msg_read[1].message == msg
    assert msg_read[1].msg_id == msg_id_2


def test_read_batch_empty_queue(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    msg_read = pgmq.read_batch(queue_name, 3)
    assert msg_read is None


def test_send_batch(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    msg = {
        "foo": "bar",
        "hello": "world",
    }
    msg_ids = pgmq.send_batch(queue_name=queue_name, messages=[msg, msg, msg])
    assert len(msg_ids) == 3
    assert msg_ids == [1, 2, 3]


def test_send_batch_with_read_batch(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    msg = {
        "foo": "bar",
        "hello": "world",
    }
    msg_ids = pgmq.send_batch(queue_name=queue_name, messages=[msg, msg, msg])
    assert len(msg_ids) == 3
    assert msg_ids == [1, 2, 3]
    msg_read_batch = pgmq.read_batch(queue_name, 3)
    assert len(msg_read_batch) == 3
    assert [msg_read.message for msg_read in msg_read_batch] == [msg, msg, msg]
    assert [msg_read.msg_id for msg_read in msg_read_batch] == [1, 2, 3]
