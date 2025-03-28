"""This module contains the functionality for extending & generating graph data.

NOTE: We create the Edge & Node classes sans __bind_orm__ b/c linters will complain if
we use variables in type hints. Users of dynamically created orm bases
(see psqlgraph.ext) will most likely want to use AbstractNode/AbstractEdge for type
hinting purposes.
"""

from collections.abc import Iterable, Iterator
from typing import TypedDict, TypeVar

from sqlalchemy.ext import associationproxy, declarative

from psqlgraph.graph import abstract

TGraphEntity = TypeVar("TGraphEntity", abstract.AbstractEdge, abstract.AbstractNode)
Base = declarative.declarative_base()


class Graph(TypedDict):
    """A unique abstract edge & node class which via their descendants define a graph."""

    edge: type[abstract.AbstractEdge]
    node: type[abstract.AbstractNode]


class Edge(abstract.AbstractEdge, declarative.AbstractConcreteBase, Base, is_abstract=True):
    """A builtin base edge class for constructing an object graph"""


class Node(abstract.AbstractNode, declarative.AbstractConcreteBase, Base, is_abstract=True):
    """A builtin base node class for constructing an object graph"""


# BIND THE NODE & EDGE CLASS TOGETHER
Edge.__edge_class__ = Edge
Edge.__node_class__ = Node
Node.__edge_class__ = Edge
Node.__node_class__ = Node


def __bind_orm__(
    orm_base: type,
) -> tuple[type[abstract.AbstractEdge], type[abstract.AbstractNode]]:
    """Binds a edge and node type to the given ORM & each other.

    Args:
        orm_base: An ORM/declarative base on which to base the new Edge and Node
            types.

    Returns:
        A edge and node type which are subclasses of `AbstractEdge` and `AbstractNode`
        types respectively as well as `declarative.AbstractConcreteBase` and the given
        `orm_base`. Further, each new type is marked as an abstract base.
    """

    class Edge(
        abstract.AbstractEdge, declarative.AbstractConcreteBase, orm_base, is_abstract=True
    ): ...

    class Node(
        abstract.AbstractNode, declarative.AbstractConcreteBase, orm_base, is_abstract=True
    ): ...

    Edge.__edge_class__ = Edge
    Node.__edge_class__ = Edge
    Edge.__node_class__ = Node
    Node.__node_class__ = Node

    return (Edge, Node)


def _get_descendants(cls: type[TGraphEntity]) -> Iterator[type[TGraphEntity]]:
    """Gets all descendant subtypes from the given class.

    Args:
        cls: The class whose concrete descendants should be returned.

    Yields:
        The descendants of cls.
    """
    for sub_cls in cls.get_subclasses():
        if not sub_cls.is_abstract_base():
            yield sub_cls  # type: ignore

        yield from _get_descendants(sub_cls)


def _create_proxy(
    src: type[abstract.AbstractNode],
    edge: type[abstract.AbstractEdge],
    dst: type[abstract.AbstractNode],
) -> None:
    """Creates an association proxy between the two nodes via the edge.

    This is were we populate linked node properties. e.g. `project.cases` &
    `case.projects` in this example this function is what creates the `cases` &
    `projects` attributes respectively.

    Args:
        src: The source node which is linked to the dst via the given edge.
        edge: The edge which connects the given src & dst nodes.
        dst: The destination node to which the src node is lined via the edge.
    """
    if not hasattr(dst, edge.__dst_src_assoc__):
        setattr(
            dst,
            edge.__dst_src_assoc__,
            associationproxy.association_proxy(
                edge.__name_in__, "src", creator=lambda node: edge(src=node)
            ),
        )

    if not hasattr(src, edge.__src_dst_assoc__):
        setattr(
            src,
            edge.__src_dst_assoc__,
            associationproxy.association_proxy(
                edge.__name_out__, "dst", creator=lambda node: edge(dst=node)
            ),
        )


def _relate_nodes(
    abstract_edge: type[abstract.AbstractEdge], abstract_node: type[abstract.AbstractNode]
) -> None:
    """Uses edges to relate the nodes to each other in the given graph.

    This function governs how the nodes are able to reference other nodes via their
    associated edges. This needs to be called after all entities in the graph have been
    defined to insure that all are correctly associated with the configured types.

    Args:
        abstract_edge: The abstract edge which is the base of the graph edges.
        abstract_node: The abstract node which is the base of the graph nodes.
    """
    edges = frozenset(_get_descendants(abstract_edge))
    nodes = {c.__name__: c for c in _get_descendants(abstract_node)}

    for edge in edges:
        src_node = nodes[edge.__src_class__]
        dst_node = nodes[edge.__dst_class__]

        _create_proxy(src_node, edge, dst_node)

    for node in nodes.values():
        node._edges_out = tuple(e.__name_out__ for e in edges if e.__src_class__ == node.__name__)
        node._edges_in = tuple(e.__name_in__ for e in edges if e.__dst_class__ == node.__name__)


def configure_graph(graphs: Iterable[Graph]) -> None:
    """Configures the graph structure of the given graphs after they have been defined.

    Args:
        graph: All graphs which need to be configured with their their final relations.
    """
    for graph in graphs:
        _relate_nodes(graph["edge"], graph["node"])
