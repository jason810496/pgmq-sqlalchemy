import pytest
from pgmq_sqlalchemy import PGMQueue


@pytest.mark.parametrize("pgmq_fixture", [
    "pgmq_by_dsn",
    "pgmq_by_async_dsn",
    "pgmq_by_engine",
    "pgmq_by_async_engine",
    "pgmq_by_session_maker",
    "pgmq_by_async_session_maker",
    "pgmq_by_dsn_and_engine",
    "pgmq_by_dsn_and_session_maker",
])
def test_construct_pgmq(pgmq_fixture,request):
    pgmq:PGMQueue = request.getfixturevalue(pgmq_fixture)
    assert pgmq is not None