# -*- coding: utf-8 -*-
"""
psqlgraph.copier.misc
----------------------------------

Misc. sqlalchemy functionality.
"""

from collections import namedtuple

from .const import (
    DEFAULT_BATCH_SIZE,
)

NodeTuple = namedtuple('NodeTuple', 'node_id,created,acl,props,sysan')
EdgeTuple = namedtuple('EdgeTuple', 'src_id,dst_id,created,acl,props,sysan')


def chunk_node_tuple_query(query, chunk_size=DEFAULT_BATCH_SIZE):
    """Given a :param:`query` object, yield chunks of
    :param:`chunk_size` of NodeTuple objects

    """
    chunk = []

    entity = query.entity()

    query = query.with_entities(
        entity.node_id,
        entity.created,
        entity.acl,
        entity._props,
        entity._sysan,
    )

    for result in query.yield_per(chunk_size):
        chunk.append(NodeTuple._make(result))

        if len(chunk) == chunk_size:
            yield chunk
            chunk = []

    if chunk:
        yield chunk


def chunk_edge_tuple_query(query, chunk_size=DEFAULT_BATCH_SIZE):
    """Given a :param:`query` object, yield chunks of
    :param:`chunk_size` of EdgeTuple objects

    """
    chunk = []

    entity = query.entity()

    query = query.with_entities(
        entity.src_id,
        entity.dst_id,
        entity.created,
        entity.acl,
        entity._props,
        entity._sysan,
    )

    for result in query.yield_per(chunk_size):
        chunk.append(EdgeTuple._make(result))

        if len(chunk) == chunk_size:
            yield chunk
            chunk = []

    if chunk:
        yield chunk
