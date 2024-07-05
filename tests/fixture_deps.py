import pytest

"""
usage:

from tests.fixture_deps import pgmq_deps

@pgmq_deps
def test_create_queue(pgmq_fixture,db_session):
    pgmq:PGMQueue = pgmq_fixture
    # test code here

note: `pytest` version should < 8.0.0,
or `pytest-lazy-fixture` will not work
ref: https://github.com/TvoroG/pytest-lazy-fixture/issues/65
"""

pgmq_deps = pytest.mark.parametrize(
    "pgmq_fixture",
    [
        pytest.lazy_fixture("pgmq_by_dsn"),
        pytest.lazy_fixture("pgmq_by_async_dsn"),
        pytest.lazy_fixture("pgmq_by_engine"),
        pytest.lazy_fixture("pgmq_by_async_engine"),
        pytest.lazy_fixture("pgmq_by_session_maker"),
        pytest.lazy_fixture("pgmq_by_async_session_maker"),
        pytest.lazy_fixture("pgmq_by_dsn_and_engine"),
        pytest.lazy_fixture("pgmq_by_dsn_and_session_maker"),
    ],
)
