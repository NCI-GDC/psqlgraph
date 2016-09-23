# -*- coding: utf-8 -*-
"""
test_postgres_copier
----------------------------------

Test the functionality to copy query results between databases.
"""

from psqlgraph.copier.schemas import copy_nodes, copy_edges
from data import insert
from models import StateMemberOfCity, City, State

import pytest


@pytest.fixture
def dst_graph(schema2_driver):
    insert(schema2_driver)
    schema2_driver.current_session().commit()


@pytest.fixture
def src_graph(db_driver):
    insert(db_driver)
    db_driver.current_session().commit()


@pytest.fixture
def dst_nodes(db_driver, schema2_driver, schema2):
    copy_nodes(db_driver, 'public', schema2, db_driver.nodes())
    db_driver.current_session().commit()


def test_node_copy_filtered(db_driver, schema2_driver, schema2, src_graph):
    expected = db_driver.nodes(State).first()
    query = db_driver.nodes(State).ids(expected.node_id)
    copy_nodes(db_driver, 'public', schema2, query)
    db_driver.current_session().commit()
    actual = schema2_driver.nodes(State).one()
    assert expected == actual


def test_node_copy_filtered_props(db_driver, schema2_driver, schema2, src_graph):
    expected = db_driver.nodes(State).first()
    query = db_driver.nodes(State).props(expected.props)
    copy_nodes(db_driver, 'public', schema2, query)
    db_driver.current_session().commit()
    actual = schema2_driver.nodes(State).one()
    assert expected == actual


def test_node_copy_base_class(db_driver, schema2_driver, schema2, src_graph):
    expected = db_driver.nodes().all()
    copy_nodes(db_driver, 'public', schema2, db_driver.nodes())
    db_driver.current_session().commit()
    actual = schema2_driver.nodes().all()
    assert expected == actual


def test_node_copy_all_overlap(db_driver, schema2_driver, schema2, src_graph):
    copy_nodes(db_driver, 'public', schema2, db_driver.nodes(State))
    db_driver.current_session().commit()
    expected = db_driver.nodes(State).count()
    actual = schema2_driver.nodes(State).count()
    assert expected == actual


def test_node_copy_some_overlap(db_driver, schema2_driver, schema2, src_graph, dst_graph):
    case_count = schema2_driver.nodes(State).count()
    to_delete = schema2_driver.nodes(State).limit(case_count/2).all()
    map(schema2_driver.current_session().delete, to_delete)
    schema2_driver.current_session().commit()
    copy_nodes(db_driver, 'public', schema2, db_driver.nodes(State))
    db_driver.current_session().commit()
    expected = db_driver.nodes(State).count()
    actual = schema2_driver.nodes(State).count()
    assert expected == actual


def test_edge_copy_no_overlap(db_driver, schema2_driver, schema2, src_graph, dst_nodes):
    copy_edges(db_driver, 'public', schema2, db_driver.edges(StateMemberOfCity))
    db_driver.current_session().commit()
    expected = db_driver.edges(StateMemberOfCity).count()
    actual = schema2_driver.edges(StateMemberOfCity).count()
    assert expected == actual


def test_edge_copy_base_class(db_driver, schema2_driver, schema2, src_graph, dst_nodes):
    copy_edges(db_driver, 'public', schema2, db_driver.edges())
    db_driver.current_session().commit()
    expected = db_driver.edges().count()
    actual = schema2_driver.edges().count()
    assert expected == actual


def test_edge_copy_all_overlap(db_driver, schema2_driver, schema2, src_graph, dst_graph):
    copy_edges(db_driver, 'public', schema2, db_driver.edges(StateMemberOfCity))
    db_driver.current_session().commit()
    expected = db_driver.edges(StateMemberOfCity).count()
    actual = schema2_driver.edges(StateMemberOfCity).count()
    assert expected == actual


def test_edge_copy_some_overlap(db_driver, schema2_driver, schema2, src_graph, dst_graph):
    case_count = schema2_driver.edges(StateMemberOfCity).count()
    to_delete = (schema2_driver.edges(StateMemberOfCity).limit(case_count/2).all())
    map(schema2_driver.current_session().delete, to_delete)
    schema2_driver.current_session().commit()
    copy_edges(db_driver, 'public', schema2, db_driver.edges(StateMemberOfCity))
    db_driver.current_session().commit()
    expected = db_driver.edges(StateMemberOfCity).count()
    actual = schema2_driver.edges(StateMemberOfCity).count()
    assert expected == actual


def test_edge_copy_missing_foreign_keys(db_driver, schema2_driver, schema2, src_graph, dst_graph):
    db_driver.engine.echo = True
    state_count = schema2_driver.edges(State).count()
    to_delete = (schema2_driver.edges(State).limit(state_count/2).all())
    map(schema2_driver.current_session().delete, to_delete)
    schema2_driver.current_session().commit()
    copy_edges(db_driver, 'public', schema2, db_driver.edges(StateMemberOfCity))
    db_driver.current_session().commit()
    actual = schema2_driver.edges(StateMemberOfCity).count()
    expected = state_count - state_count/2
    assert expected == actual
