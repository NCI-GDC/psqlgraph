# -*- coding: utf-8 -*-
"""
psqlgraph.copier.upsert
----------------------------------

Functionality for bulk UPSERT implemented at application level.
"""

from sqlalchemy import and_, or_, not_, bindparam
from psqlgraph import Node
from logging import getLogger

from .const import (
    DEFAULT_BATCH_SIZE,
)

logger = getLogger("psqlgraph.copier.upsert")


def get_node_values(node):
    """Expand node properties for SQL parameters. Returns something you
    can use as `execute` parameters.

    """

    return dict(
        node_id=node.node_id,
        created=node.created,
        _props=node.props,
        _sysan=node.sysan,
        acl=node.acl
    )


def get_edge_values(edge):
    """Expand edge properties for SQL parameters. Returns something you
    can use as `execute` parameters.

    """

    return dict(
        src_id=edge.src_id,
        dst_id=edge.dst_id,
        created=edge.created,
        _props=edge.props,
        _sysan=edge.sysan,
        acl=edge.acl
    )


def update_nodes(db, node_type, nodes):
    """UPDATE :param:`nodes` into :param:`session`

    ..note:: Limited to a single node_type

    """

    logger.info("Emitting UPDATE for %s %s nodes",
                len(nodes), node_type.__name__)

    if not nodes:
        return

    table = node_type.__table__
    where_statement = table.c.node_id == bindparam('bindparam')
    update_node_values = [
        dict(bindparam=node.node_id, **get_node_values(node))
        for node in nodes
    ]

    db.current_session().execute(
        table.update().where(where_statement),
        update_node_values,
    )


def update_edges(db, edge_type, edges):
    """UPDATE :param:`edges` into :param:`session`

    ..note:: Limited to a single edge_type

    """

    logger.info("Emitting UPDATE for %s %s edges",
                len(edges), edge_type.__name__)

    if not edges:
        return

    table = edge_type.__table__
    where_statement = and_(
        table.c.src_id == bindparam('bindparam_src_id'),
        table.c.dst_id == bindparam('bindparam_dst_id'),
    )
    update_edge_values = [
        dict(
            bindparam_src_id=edge.src_id,
            bindparam_dst_id=edge.dst_id,
            **get_edge_values(edge)
        )
        for edge in edges
    ]

    db.current_session().execute(
        table.update().where(where_statement),
        update_edge_values,
    )


def insert_nodes(db, node_type, nodes):
    """INSERT :param:`nodes` into :param:`session`

    ..note:: Limited to a single node_type

    """

    logger.info("Emitting INSERT for %s %s nodes",
                len(nodes), node_type.__name__)

    if not nodes:
        return

    table = node_type.__table__
    new_node_values = [get_node_values(node) for node in nodes]

    db.current_session().execute(
        table.insert(),
        new_node_values,
    )


def insert_edges(db, edge_type, edges, skip_missing_foreign_key=True):
    """INSERT :param:`edges` into :param:`session`

    ..note:: Limited to a single edge_type

    """

    logger.info("Emitting INSERT for %s %s edges",
                len(edges), edge_type.__name__)

    if not edges:
        return

    table = edge_type.__table__
    session = db.current_session()

    if skip_missing_foreign_key:
        missing_src_ids, missing_dst_ids = get_missing_foreign_keys(
            db, edge_type, edges)
    else:
        missing_src_ids, missing_dst_ids = [], []

    new_edge_values = [
        get_edge_values(edge)
        for edge in edges
        if (edge.src_id,) not in missing_src_ids
        and (edge.dst_id,) not in missing_dst_ids
    ]

    session.execute(
        table.insert(),
        new_edge_values,
    )


def get_existing_node_ids(db, node_type, nodes):
    """Returns the intersection of node_id that exist in database"""

    logger.info("Checking which of %s %s nodes already exist",
                len(nodes), node_type.__name__)

    source_ids = [node.node_id for node in nodes]

    result = set(db.nodes(node_type)
                 .filter(node_type.node_id.in_(source_ids))
                 .with_entities(node_type.node_id)
                 .yield_per(DEFAULT_BATCH_SIZE))

    logger.info("Found existing %s %s nodes",
                len(result), node_type.__name__)

    return result


def get_missing_foreign_keys(db, edge_type, edges):
    """Returns the missing of foreign keys from database for edges"""

    logger.info("Checking which of %s %s edges already exist",
                len(edges), edge_type.__name__)

    src_cls = Node.get_subclass_named(edge_type.__src_class__)
    dst_cls = Node.get_subclass_named(edge_type.__dst_class__)

    src_ids = [edge.src_id for edge in edges]
    dst_ids = [edge.dst_id for edge in edges]

    missing_src_ids = set(db.edges(edge_type)
                          .filter(not_(edge_type.src_id.in_(src_ids)))
                          .with_entities(edge_type.src_id)
                          .yield_per(DEFAULT_BATCH_SIZE))

    missing_dst_ids = set(db.edges(edge_type)
                          .filter(not_(edge_type.dst_id.in_(dst_ids)))
                          .with_entities(edge_type.dst_id)
                          .yield_per(DEFAULT_BATCH_SIZE))

    logger.info("Found missing %s %s src foreign keys",
                len(missing_src_ids), src_cls.__name__)
    logger.info("Found missing %s %s dst foreign keys",
                len(missing_dst_ids), dst_cls.__name__)

    return missing_src_ids, missing_dst_ids


def get_existing_edge_ids(db, edge_type, edges):
    """Returns the intersection of edge_id that exist in database"""

    logger.info("Checking which of %s %s edges already exist",
                len(edges), edge_type.__name__)

    possible_ids = [(edge.src_id, edge.dst_id) for edge in edges]

    result = set(db.edges(edge_type)
                 .filter(or_(*[and_(
                     edge_type.src_id == src_id,
                     edge_type.dst_id == dst_id,
                 ) for (src_id, dst_id) in possible_ids]))
                 .with_entities(edge_type.src_id, edge_type.dst_id)
                 .yield_per(DEFAULT_BATCH_SIZE))

    logger.info("Found existing %s %s edges",
                len(result), edge_type.__name__)

    return result


def upsert_nodes(db, node_type, nodes):
    """

    """

    existing_ids = get_existing_node_ids(db, node_type, nodes)

    update_nodes(db, node_type, [
        node
        for node in nodes
        if (node.node_id,) in existing_ids
    ])

    insert_nodes(db, node_type, [
        node
        for node in nodes
        if (node.node_id,) not in existing_ids
    ])


def upsert_edges(db, edge_type, edges, skip_missing_foreign_key=True):
    """

    """

    existing_ids = get_existing_edge_ids(db, edge_type, edges)

    update_edges(db, edge_type, [
        edge
        for edge in edges
        if (edge.src_id, edge.dst_id) in existing_ids
    ])

    insert_edges(db, edge_type, [
        edge
        for edge in edges
        if (edge.src_id, edge.dst_id) not in existing_ids
    ], skip_missing_foreign_key)
