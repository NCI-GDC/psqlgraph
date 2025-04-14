import inspect

import pytest
from sqlalchemy import orm

import psqlgraph
from psqlgraph import ext

SNode, SEdge = ext.register_base_class("sample")
SBase = ext.get_orm_base("sample")

TNode, TEdge = ext.register_base_class("test")
TBase = ext.get_orm_base("test")

globals()["SNode"] = SNode


def foo(node: SNode) -> int: ...


@pytest.mark.parametrize(
    "ns",
    ("sample", "test", None),
)
def test_register_bases(ns):

    node_cls, edge_cls = ext.register_base_class(package_namespace=ns)
    assert issubclass(node_cls, psqlgraph.AbstractNode)
    assert issubclass(edge_cls, psqlgraph.AbstractEdge)


def test_base_classes_distinct():
    def create_node(node_base: type[psqlgraph.AbstractNode]) -> type[psqlgraph.AbstractNode]:
        class AlignedReads(node_base):
            __label__ = "aligned_reads"
            __tablename__ = "node_aligned_reads"

            @psqlgraph.pg_property(float)
            def height(self, value):
                self._set_property("height", value)

        # Insure that configuration can be called for both.
        orm.configure_mappers()

        return AlignedReads

    # Check that the base classes are not the same.
    assert SNode != TNode
    assert SEdge != TEdge

    sample = create_node(SNode)
    test = create_node(TNode)

    # Insure that despite same name classes are not same & that they are not
    # contaminated with the opposites base class.
    assert sample.__name__ == test.__name__
    assert sample != test
    assert TNode not in inspect.getmro(sample)
    assert SNode in inspect.getmro(sample)
    assert SNode not in inspect.getmro(test)
    assert TNode in inspect.getmro(test)


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
