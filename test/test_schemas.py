# -*- coding: utf-8 -*-
"""
psqlgraph.test.test_schemas
----------------------------------

Test functionality dealing with Postgres schemas

"""

from psqlgraph import PsqlGraphDriver

from models import Test

import pytest


def test_default_search_path(db_driver):
    with db_driver.session_scope() as session:
        expected = '"$user",public'
        actual = session.execute('show search_path').fetchall()
        assert actual[0][0].replace(' ', '') == expected


def test_custom_search_path(schema2_driver, schema2):
    with schema2_driver.session_scope() as session:
        assert session.execute('show search_path').fetchall() == [(schema2,)]


def test_custom_search_path_isolation(schema2_driver, db_driver):
    with db_driver.session_scope() as session:
        session.merge(Test('public_test'))

    with db_driver.session_scope() as session:
        assert db_driver.nodes(Test).count() == 1

    with schema2_driver.session_scope() as session:
        assert schema2_driver.nodes(Test).count() == 0


def test_custom_search_path_insertion(schema2_driver, db_driver):
    with schema2_driver.session_scope() as session:
        session.merge(Test('schema_test'))

    with schema2_driver.session_scope() as session:
        assert schema2_driver.nodes(Test).count() == 1

    with db_driver.session_scope() as session:
        assert db_driver.nodes(Test).count() == 0
