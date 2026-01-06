import pytest
from pgmq_sqlalchemy import PGMQueue

from tests.fixture_deps import pgmq_all_variants

use_fixtures = [pgmq_all_variants]


def test_construct_pgmq(pgmq_all_variants):
    pgmq: PGMQueue = pgmq_all_variants
    assert pgmq is not None


def test_construct_invalid_pgmq():
    with pytest.raises(ValueError) as e:
        _ = PGMQueue()
    error_msg: str = str(e.value)
    assert "Must provide either dsn, engine, or session_maker" in error_msg
