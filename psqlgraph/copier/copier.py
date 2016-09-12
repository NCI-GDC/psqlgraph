# -*- coding: utf-8 -*-
"""
psqlgraph.copier.copier
----------------------------------

Functionality to stream result set from source database to target
database.

"""

from collections import defaultdict
from psqlgraph import Node, Edge
from sqlalchemy import or_, bindparam
from sqlalchemy.orm import joinedload
from logging import getLogger


from .upsert import (
    upsert_nodes,
    upsert_edges,
)

from .misc import (
    chunk_edge_tuple_query,
    chunk_node_tuple_query,
)

logger = getLogger("psqlgraph.copier")


def copy_nodes(destination_db, source_query):
    """Copies the results of :param:`source_query` form :param:`source_db`
    to :param:`destination_db`. Copied results will include the
    selected entity in the query and all of the edges that are defined
    on that object

    :returns: The number of results copied

    """

    entity_type = source_query.entity()

    if not issubclass(entity_type, Node) or entity_type is Node:
        raise RuntimeError("Type {} must be subclass of Node"
                           .format(entity_type))

    print source_query.all()

    for chunk in chunk_node_tuple_query(source_query):
        upsert_nodes(destination_db, entity_type, chunk)


def copy_edges(destination_db, source_query):
    """Copies the results of :param:`source_query` form :param:`source_db`
    to :param:`destination_db`. Copied results will include the
    selected entity in the query and all of the edges that are defined
    on that object

    :returns: The number of results copied

    """

    entity_type = source_query.entity()

    if not issubclass(entity_type, Edge) or entity_type is Edge:
        raise RuntimeError("Type {} must be subclass of Edge"
                           .format(entity_type))

    for chunk in chunk_edge_tuple_query(source_query):
        upsert_edges(destination_db, entity_type, chunk)


def eagerly_reload_nodes(db, nodes):
    """Reload nodes. This is a tradeoff in performance to reload
    all of the nodes, with the idea that the edges for all of
    them will be eagerly loaded and getting all of the edges
    will be faster.

    """

    ids = [n.node_id for n in nodes]

    return db.nodes().ids(ids).options(subqueryload('*'))


def copy_subgraph(destination_db, nodes):
    """Copy :param:`nodes` and all their edges to :param:`destination_db`
    database

    """

    # Group nodes by class
    grouped_nodes = defaultdict(list)
    for node in nodes:
        grouped_nodes[node.__class__].append(node)

    # Group edges by class
    edges = chain((n.get_edges() for n in nodes))
    grouped_edges = defaultdict(list)
    for edge in edges:
        grouped_edges[edge.__class__].append(edge)

    with destination_db.session_scope():
        # Snapshot nodes
        for node_type, type_nodes in grouped_nodes.iteritems():
            upsert_nodes(destination_db, node_type, type_nodes)

        # Snapshot edges
        for edge_type, type_edges in grouped_edges:
            upsert_edges(destination_db, edge_type, type_edges)
