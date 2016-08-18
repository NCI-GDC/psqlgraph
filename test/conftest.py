# -*- coding: utf-8 -*-
"""
Setup psqlgraph tests
"""

import models
from psqlgraph import PsqlGraphDriver, Node, Edge

import pytest


# Source database config
PG_SOURCE_HOST = 'localhost'
PG_SOURCE_USER = 'test'
PG_SOURCE_PASSWORD = 'test'
PG_SOURCE_DATABASE = 'automated_test'

# Destination database config
PG_DESTINATION_HOST = 'localhost'
PG_DESTINATION_USER = 'test'
PG_DESTINATION_PASSWORD = 'test'
PG_DESTINATION_DATABASE = 'automated_test_destination'

# Source database driver
_source_db = PsqlGraphDriver(
    PG_SOURCE_HOST,
    PG_SOURCE_USER,
    PG_SOURCE_PASSWORD,
    PG_SOURCE_DATABASE,
)

# Destination database driver
_destination_db = PsqlGraphDriver(
    PG_DESTINATION_HOST,
    PG_DESTINATION_USER,
    PG_DESTINATION_PASSWORD,
    PG_DESTINATION_DATABASE,
)


@pytest.fixture
def clear_database(db):
    edge_tables = Edge.get_subclass_table_names()
    node_tables = Node.get_subclass_table_names()

    tables = [
        t for t in edge_tables + node_tables
        if t not in {'edge_edge', 'node_node'}
    ]

    with db.engine.begin() as conn:
        conn.execute(';'.join([
            'DELETE FROM {}'.format(table)
            for table in tables
        ]))


def temp_transaction_db(db):
    """Yield a db driver that rolls back standard commits.

    Call db.current_session()._commit() to commit to database

    """

    with db.session_scope() as session:
        session.commit, session._commit = session.flush, session.commit
        yield db
        session.commit = session._commit
        session.rollback()


@pytest.yield_fixture
def source_db():
    with _source_db.session_scope():
        clear_database(_source_db)
        yield _source_db
        _source_db.current_session().rollback()
        clear_database(_source_db)


@pytest.yield_fixture
def destination_db():
    with _destination_db.session_scope():
        clear_database(_destination_db)
        yield _destination_db
        _destination_db.current_session().rollback()
        clear_database(_destination_db)
