# -*- coding: utf-8 -*-
"""
Setup psqlgraph tests
"""

import models
from psqlgraph import PsqlGraphDriver, Node, Edge

import pytest


# Source database config
@pytest.fixture
def pg_config():
    return {
        'host': 'localhost',
        'user': 'test',
        'password': 'test',
        'database': 'psqlgraph_automated_test',
        # 'echo': True,
    }


# Source database config
@pytest.fixture
def schema2():
    return 'schema2'


# database driver
_db_driver = PsqlGraphDriver(**pg_config())
_schema2_driver = PsqlGraphDriver(search_path=schema2(), **pg_config())


@pytest.fixture
def clear_database(db):
    tables = [
        table for table in Node.get_subclass_table_names()
        if table not in {'node_node'}
    ]

    with db.engine.begin() as conn:
        conn.execute(';'.join([
            'TRUNCATE {} CASCADE'.format(table)
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
def db_driver():
    with _db_driver.session_scope():
        clear_database(_db_driver)
        yield _db_driver
        _db_driver.current_session().rollback()
        clear_database(_db_driver)


@pytest.yield_fixture
def schema2_driver():
    with _schema2_driver.session_scope():
        clear_database(_schema2_driver)
        yield _schema2_driver
        _schema2_driver.current_session().rollback()
        clear_database(_schema2_driver)
