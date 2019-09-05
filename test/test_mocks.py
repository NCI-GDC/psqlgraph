import random
import re
import uuid
from collections import defaultdict

import pytest

from psqlgraph import Node
from psqlgraph.mocks import GraphFactory, NodeFactory
from test import models


STRING_MATCH = '[a-zA-Z0-9]{32}'
DATE_MATCH = '^[0-9]{4}-[0-9]{2}-[0-9]{2}T00:00:00'


class FakeModels(object):
    def __init__(self):
        self.Node = Node
        self.Test = models.Test
        self.Foo = models.Foo
        self.FooBar = models.FooBar


@pytest.fixture(scope='session')
def gdcmodels():
    return FakeModels()


@pytest.fixture(scope='session')
def gdcdictionary():
    return models.FakeDictionary()


@pytest.fixture
def node_factory(gdcmodels, gdcdictionary):
    return NodeFactory(gdcmodels, gdcdictionary.schema)


@pytest.fixture
def patched_randrange(monkeypatch):
    monkeypatch.setattr(random, 'randrange', lambda *args: 20)


def validate_test(node):
    assert isinstance(node, models.Test)
    for key in ['key1', 'key2', 'key3', 'new_key']:
        assert re.match(STRING_MATCH, getattr(node, key))
    assert re.match(DATE_MATCH, node.timestamp)


def validate_foo(node):
    assert isinstance(node, models.Foo)
    assert isinstance(node.fobble, int)
    assert 20 <= node.fobble <= 30
    assert re.match(STRING_MATCH, node.bar)
    assert node.baz in ['allowed_1', 'allowed_2']


def validate_foo_bar(node):
    assert isinstance(node, models.FooBar)
    assert re.match(STRING_MATCH, node.bar)


@pytest.mark.parametrize('label, validator', [
    ('test', validate_test),
    ('foo', validate_foo),
    ('foo_bar', validate_foo_bar),
])
def test_node_factory_all_props(node_factory, label, validator):
    node = node_factory.create(label, all_props=True)

    validator(node)


def test_node_factory_sets_required_for_test_node(node_factory):
    test_node = node_factory.create('test')
    assert re.match(STRING_MATCH, test_node.key1)
    assert all(
        getattr(test_node, key) is None
        for key in ['key2', 'key3', 'new_key', 'timestamp']
    )


def test_node_factory_doesnt_set_any_props(node_factory):
    foo_node = node_factory.create('foo')
    assert all(
        getattr(foo_node, key) is None
        for key in ['bar', 'baz', 'fobble']
    )


def test_init_graph_factory(gdcmodels, gdcdictionary):
    _ = GraphFactory(gdcmodels, gdcdictionary)


def test_graph_factory_with_nodes_and_edges(gdcmodels, gdcdictionary):
    gf = GraphFactory(gdcmodels, gdcdictionary)

    foobar_uuids = [str(uuid.uuid4())]
    foo_uuids = [str(uuid.uuid4()), str(uuid.uuid4())]
    test_uuids = [str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())]

    nodes = [
        {'label': 'test', 'node_id': test_uuids[0]},
        {'label': 'test', 'node_id': test_uuids[1]},
        {'label': 'test', 'node_id': test_uuids[2]},
        {'label': 'foo', 'node_id': foo_uuids[0]},
        {'label': 'foo', 'node_id': foo_uuids[1]},
        {'label': 'foo_bar', 'node_id': foobar_uuids[0]},
    ]

    edges = [
        {'src': test_uuids[0], 'dst': test_uuids[1]},       # t0 -> t1
        {'src': test_uuids[0], 'dst': foo_uuids[0]},        # t0 -> f0
        {'src': test_uuids[1], 'dst': foo_uuids[1]},        # t1 -> f1
        {'src': test_uuids[2], 'dst': foo_uuids[1]},        # t2 -> f1
        {'src': test_uuids[0], 'dst': foobar_uuids[0]},     # invalid edge
        {'src': foo_uuids[0], 'dst': foobar_uuids[0]},      # f0 -> fb0
        {'src': foo_uuids[1], 'dst': foobar_uuids[0]},      # f1 -> fb0
    ]

    created_nodes = gf.create_from_nodes_and_edges(nodes=nodes, edges=edges,
                                                   unique_key='node_id')

    expected_adjacency = defaultdict(set)
    for edge_info in edges:
        if edge_info['dst'] == foobar_uuids[0] and \
                edge_info['src'] == test_uuids[0]:
            # This edge shouldn't exist
            continue
        expected_adjacency[edge_info['dst']].add(edge_info['src'])

    nodes_map = {}
    for node in created_nodes:
        nodes_map[node.node_id] = node

    # make sure that number of nodes is as expected
    assert len(created_nodes) == len(nodes)

    # make sure the links are correct
    for dst_id, src_ids in expected_adjacency.items():
        node = nodes_map[dst_id]
        assert {edge.src.node_id for edge in node.edges_in} == src_ids


def assert_all_node_types_created_once(nodes):
    # since random.randrange was patched, it's guaranteed that all children
    # will be visited, so we expect 3 nodes 1 for each type
    type_counts = defaultdict(int)
    for n in nodes:
        type_counts[n.label] += 1

    assert len(nodes) == 3
    assert len(type_counts) == 3
    assert {'foo', 'test', 'foo_bar'} == set(type_counts.keys())


def test_graph_factory_random_subgraph(gdcmodels, gdcdictionary,
                                       patched_randrange):
    gf = GraphFactory(gdcmodels, gdcdictionary)

    nodes = gf.create_random_subgraph('foo_bar')

    assert_all_node_types_created_once(nodes)


def test_graph_factory_with_globals(gdcmodels, gdcdictionary,
                                    patched_randrange):

    graph_globals = {
        'properties': {
            'key1': 'abcdefghijklmnopqrstuvwxyz012345',
            'baz': 'disallowed',
            'bar': '012345abcdefghijklmnopqrstuvwxyz',
        }
    }

    gf = GraphFactory(gdcmodels, gdcdictionary, graph_globals=graph_globals)

    nodes = gf.create_random_subgraph('foo_bar', all_props=True)

    assert_all_node_types_created_once(nodes)

    prop_counts = defaultdict(int)
    for n in nodes:
        for key_val in n.props.items():
            prop_counts[key_val] += 1

    # 'disallowed' is not in Enum and shouldn't be set
    assert ('baz', 'disallowed') not in prop_counts
    # 2 nodes have property 'bar' and should be set
    assert prop_counts.pop(('bar', '012345abcdefghijklmnopqrstuvwxyz')) == 2
    assert all([val == 1 for val in prop_counts.values()]), prop_counts


def test_graph_factory_with_override_globals(gdcmodels, gdcdictionary):

    graph_globals = {
        'properties': {
            'baz': 'allowed_1',
            'bar': '012345abcdefghijklmnopqrstuvwxyz'
        }
    }

    gf = GraphFactory(gdcmodels, gdcdictionary, graph_globals=graph_globals)

    nodes = [
        dict(label='foo', node_id='id_1'),
        dict(label='foo', node_id='id_2', baz='allowed_2', bar='hello', fobble=30),
        dict(label='foo', node_id='id_3', baz='disallowed'),
        dict(label='foo', node_id='id_4', bar=1, fobble='hello'),
    ]

    # valid passed values get set
    # invalid passed values are overridden by global if set
    # otherwise, we use random valid value
    expected_values = [
        dict(node_id='id_1', bar='012345abcdefghijklmnopqrstuvwxyz', baz='allowed_1'),
        dict(node_id='id_2', bar='hello', baz='allowed_2', fobble=30),
        dict(node_id='id_3', bar='012345abcdefghijklmnopqrstuvwxyz', baz='allowed_1'),
        dict(node_id='id_4', bar='012345abcdefghijklmnopqrstuvwxyz', baz='allowed_1')
    ]

    created_nodes = gf.create_from_nodes_and_edges(nodes, edges=[], all_props=True,
                                                   unique_key='node_id')
    created_nodes = sorted(created_nodes, key=lambda x: x.node_id)
    for created_node, expected in zip(created_nodes, expected_values):
        for k, v in expected.items():
            assert created_node[k] == v
