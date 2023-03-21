import ext
import pytest

from psqlgraph.edge import AbstractEdge, Edge
from psqlgraph.node import AbstractNode, Node


@pytest.mark.parametrize(
    "ns, node_cls_name, edge_cls_name",
    [
        ("sample", "SampleAbstractNode", "SampleAbstractEdge"),
        ("test", "TestAbstractNode", "TestAbstractEdge"),
        (None, "Node", "Edge"),
    ],
)
def test_register_bases(ns, node_cls_name, edge_cls_name):

    node_cls, edge_cls = ext.register_base_class(package_namespace=ns)
    assert issubclass(node_cls, AbstractNode)
    assert issubclass(edge_cls, AbstractEdge)

    assert node_cls.__name__ == node_cls_name
    assert edge_cls.__name__ == edge_cls_name


@pytest.mark.parametrize("ns", ["sample", "test", None])
def test_bases_cached(ns):
    node_cls, edge_cls = ext.register_base_class(package_namespace=ns)

    node_cls_1, edge_cls_1 = ext.register_base_class(package_namespace=ns)
    assert node_cls == node_cls_1
    assert edge_cls == edge_cls_1


def test_abstract_defaults():
    node_cls = ext.get_abstract_node()
    assert node_cls == Node

    edge_cls = ext.get_abstract_edge()
    assert edge_cls == Edge
