import uuid

import pytest
from sqlalchemy import text
from pgmq_sqlalchemy import PGMQueue


@pytest.mark.parametrize("pgmq_fixture", [
    "pgmq_by_dsn",
    "pgmq_by_async_dsn",
    "pgmq_by_engine",
    # "pgmq_by_async_engine",
    "pgmq_by_session_maker",
    # "pgmq_by_async_session_maker",
    "pgmq_by_dsn_and_engine",
    "pgmq_by_dsn_and_session_maker",
])
def test_create_queue(pgmq_fixture,db_session,request):
    pgmq:PGMQueue = request.getfixturevalue(pgmq_fixture)
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    pgmq.create_queue(queue_name)
    row = db_session.execute(text("SELECT queue_name FROM pgmq.list_queues() WHERE queue_name = :queue_name ;"), {"queue_name":queue_name}).first()
    assert row is not None
    assert row[0] == queue_name
