# -*- coding: utf-8 -*-
"""
test_postgres_copier
----------------------------------

Test the functionality to copy query results between databases.
"""

from psqlgraph.copier.copier import copy_nodes, copy_edges
from data import insert
from models import StateMemberOfCity, City, State

import pytest


@pytest.fixture
def dst_graph(destination_db):
    insert(destination_db)


@pytest.fixture
def src_graph(source_db):
    insert(source_db)


@pytest.fixture
def dst_nodes(source_db, destination_db):
    copy_nodes(destination_db, source_db.nodes(State))


def test_copy_nodes_no_overlap(source_db, destination_db, src_graph):
    copy_nodes(destination_db, source_db.nodes(State))
    actual = source_db.nodes(State).count()
    expected = destination_db.nodes(State).count()
    assert actual == expected


def test_copy_nodes_all_overlap(source_db, destination_db, src_graph):
    copy_nodes(destination_db, source_db.nodes(State))
    actual = source_db.nodes(State).count()
    expected = destination_db.nodes(State).count()
    assert actual == expected


def test_copy_nodes_some_overlap(source_db, destination_db, src_graph):
    case_count = destination_db.nodes(State).count()
    to_delete = destination_db.nodes(State).limit(case_count/2).all()
    map(destination_db.current_session().delete, to_delete)
    copy_nodes(destination_db, source_db.nodes(State))
    actual = source_db.nodes(State).count()
    expected = destination_db.nodes(State).count()
    assert actual == expected


def test_copy_edges_no_overlap(source_db, destination_db, src_graph, dst_nodes):
    copy_edges(destination_db, source_db.edges(StateMemberOfCity))
    actual = source_db.edges(StateMemberOfCity).count()
    expected = destination_db.edges(StateMemberOfCity).count()
    assert actual == expected


def test_copy_edges_all_overlap(source_db, destination_db, src_graph, dst_graph):
    copy_edges(destination_db, source_db.edges(StateMemberOfCity))
    actual = source_db.edges(StateMemberOfCity).count()
    expected = destination_db.edges(StateMemberOfCity).count()
    assert actual == expected


def test_copy_edges_some_overlap(source_db, destination_db, src_graph, dst_graph):
    case_count = destination_db.edges(StateMemberOfCity).count()
    to_delete = (destination_db.edges(StateMemberOfCity)
                 .limit(case_count/2).all())
    map(destination_db.current_session().delete, to_delete)
    copy_edges(destination_db, source_db.edges(StateMemberOfCity))
    actual = source_db.edges(StateMemberOfCity).count()
    expected = destination_db.edges(StateMemberOfCity).count()
    assert actual == expected


def test_copy_edges_no_foreign_keys(source_db, destination_db, src_graph):
    destination_db.edges(StateMemberOfCity).delete()
    case_count = destination_db.edges(StateMemberOfCity).count()
    to_delete = (destination_db.edges(StateMemberOfCity)
                 .limit(case_count/2).all())
    map(destination_db.current_session().delete, to_delete)
    copy_edges(destination_db, source_db.edges(StateMemberOfCity))
    actual = source_db.edges(StateMemberOfCity).count()
    expected = destination_db.edges(StateMemberOfCity).count()
    assert actual == expected
