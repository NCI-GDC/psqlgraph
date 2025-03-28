import pytest

import psqlgraph
from psqlgraph import ext


@pytest.mark.parametrize(
    "ns",
    ("sample", "test", None),
)
def test_register_bases(ns):

    node_cls, edge_cls = ext.register_base_class(package_namespace=ns)
    assert issubclass(node_cls, psqlgraph.AbstractNode)
    assert issubclass(edge_cls, psqlgraph.AbstractEdge)


@pytest.mark.parametrize("ns", ["sample", "test", None])
def test_bases_cached(ns):
    node_cls, edge_cls = ext.register_base_class(package_namespace=ns)

    node_cls_1, edge_cls_1 = ext.register_base_class(package_namespace=ns)
    assert node_cls == node_cls_1
    assert edge_cls == edge_cls_1


def test_abstract_defaults():
    node_cls = ext.get_abstract_node()
    assert node_cls == psqlgraph.Node

    edge_cls = ext.get_abstract_edge()
    assert edge_cls == psqlgraph.Edge
