"""A module for maintaining the extension of SQLAlchemy logic to define a graph ORM."""

from __future__ import annotations

import collections
import functools
from collections.abc import Iterable

import sqlalchemy
from sqlalchemy import event, orm, schema
from sqlalchemy.ext import declarative

from psqlgraph import graph, voided

_GRAPHS: dict[str | None, graph.Graph] = {None: {"node": graph.Node, "edge": graph.Edge}}
_ORM_BASES: dict[str | None, type] = collections.defaultdict(declarative.declarative_base)
_ORM_BASES[None] = graph.Base

# Add the listener for configuring the all graphs before the mapper is configured for
# them. This generally happens upon the first use of any of the defined entities
event.listen(
    orm.mapper, "before_configured", functools.partial(graph.configure_graph, _GRAPHS.values())
)


def get_orm_base(package_namespace: str | None) -> type:
    """Helper function to get the appropriate sqlalchemy base class
    Args:
       package_namespace (str): module namespace
    """
    return _ORM_BASES[package_namespace]


def get_abstract_edge(package_namespace: str | None = None) -> type[graph.AbstractEdge]:
    return _GRAPHS[package_namespace]["edge"]


def get_abstract_node(package_namespace: str | None = None) -> type[graph.AbstractNode]:
    return _GRAPHS[package_namespace]["node"]


def create_base_class(package_namespace: str) -> graph.Graph:
    """Dynamically creates an abstract base class that extends either the Node or Edge class
    Args:
        pkg_namespace (str): package namespace
        is_node (bool): if True creates a node abstract class else creates an edge
    Returns:
        class: A dynamically generated abstract class
    """
    base = get_orm_base(package_namespace)
    edge, node = graph.__bind_orm__(base)

    return {"edge": edge, "node": node}


def register_base_class(
    package_namespace: str | None = None,
) -> tuple[type[graph.AbstractNode], type[graph.AbstractEdge]]:
    """Registers or returns a registered base node and edge classes as tuple for the package namespace
        Example:
            if package_namespace = `bio`
            This function will dynamically create and cache the following classes
                * `psqlgraph.ext.BioAbstractNode`
                * `psqlgraph.ext.BioAbstractEdge`
            Custom entities can now extend these class
            >>> N, E = register_base_class("bio")
            >>> class Node1(N):
            >>>    prop = sqlalchemy.Column(sqlalchemy.String)
            >>> class Edge1(E):
            >>>    prop = sqlalchemy.Column(sqlalchemy.TEXT)
    Args:
        package_namespace (str): If None, defaults to normal Node and Edge class
    Returns:
        tuple (class, class):
    """
    if package_namespace not in _GRAPHS:
        assert package_namespace, "Cannot override default graph."

        _GRAPHS[package_namespace] = create_base_class(package_namespace)

    graph = _GRAPHS[package_namespace]

    return graph["node"], graph["edge"]


def create_all(engine: sqlalchemy.Engine, base: type = graph.Base) -> None:
    """Creates tables associated with a given declarative base and voided entities.

    Args:
        engine: The engine which can be invoked to drop the data.
        base: A declarative base class. By default this is the builtin graph.Base.
    """
    base.metadata.create_all(engine)
    voided.Base.metadata.create_all(engine)


def drop_all(engine: sqlalchemy.Engine, base: type = graph.Base) -> None:
    """Drops tables associated with a given declarative base and all voided entities.

    Args:
        engine: The engine which can be invoked to drop the data.
        base: A declarative base class. By default this is the builtin graph.Base.
    """
    tables: Iterable[schema.Table] = reversed(base.metadata.sorted_tables)

    for table in tables:
        table.drop(engine, checkfirst=True)

    voided.Base.metadata.drop_all(engine)
