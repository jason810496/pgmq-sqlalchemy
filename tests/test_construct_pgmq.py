from pgmq_sqlalchemy import PGMQueue

from tests.fixture_deps import pgmq_deps


@pgmq_deps
def test_construct_pgmq(pgmq_fixture):
    pgmq: PGMQueue = pgmq_fixture
    assert pgmq is not None
