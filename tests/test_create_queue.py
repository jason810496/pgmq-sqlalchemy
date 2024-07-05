import uuid

from sqlalchemy import text
from pgmq_sqlalchemy import PGMQueue


from tests.fixture_deps import pgmq_deps


@pgmq_deps
def test_create_queue(pgmq_fixture, db_session):
    pgmq: PGMQueue = pgmq_fixture
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    pgmq.create_queue(queue_name)
    row = db_session.execute(
        text(
            "SELECT queue_name FROM pgmq.list_queues() WHERE queue_name = :queue_name ;"
        ),
        {"queue_name": queue_name},
    ).first()
    assert row is not None
    assert row[0] == queue_name
