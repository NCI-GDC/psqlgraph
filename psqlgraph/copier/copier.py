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
