# -*- coding: utf-8 -*-
"""
migrations.edge_fk_constraint_utils.py
----------------------------------

Adds and removes foreign key constraints to edge tables (doesn't
remove or create edges).

"""


from psqlgraph import Node, Edge
from sqlalchemy import text

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def constraint_exists(connection, constraint_name):
    """Check if constraint exists"""

    return any(connection.execute(
        text("select 1 from pg_constraint where conname = :name"),
        name=constraint_name
    ))


def get_fk_name(edge_table, column):
    return "{edge_table}_{column}_fkey".format(
        edge_table=edge_table,
        column=column,
    )


def drop_edge_fk_constraint(connection, edge, column):
    """Drop the edge constraint for a given edge column (src_id or dst_id)"""

    fk_name = get_fk_name(edge.__tablename__, column)

    if not constraint_exists(connection, fk_name):
        return logger.info('Constraint %s does not exists', fk_name)

    connection.execute(
        "ALTER TABLE {table} DROP CONSTRAINT {fk_name}"
        .format(table=edge.__tablename__, fk_name=fk_name)
    )

    logger.info('Dropped constraint %s', fk_name)


def drop_edge_fk_constraints(connection, edge):
    """Drop both edge constraints for a given edge"""

    drop_edge_fk_constraint(connection, edge, 'src_id')
    drop_edge_fk_constraint(connection, edge, 'dst_id')


def add_edge_fk_constraint(connection, edge_table, column, neighbor_node):
    """Creates fk constraint for edge in given column and neighbor"""

    fk_name = get_fk_name(edge_table, column)

    if constraint_exists(connection, fk_name):
        return logger.info('Constraint %s already exists', fk_name)

    connection.execute("""
    ALTER TABLE {edge_table}
      ADD CONSTRAINT "{fk_name}"
      FOREIGN KEY ({column}) REFERENCES {node_table}(node_id)
      ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
    """.format(
        fk_name=fk_name,
        edge_table=edge_table,
        column=column,
        node_table=neighbor_node.__tablename__,
    ))

    logger.info('Constraint %s created', fk_name)


def add_edge_fk_constraints(connection, edge):
    """Creates all fk constraints for edge"""

    src_cls = Node.get_subclass_named(edge.__src_class__)
    dst_cls = Node.get_subclass_named(edge.__dst_class__)

    add_edge_fk_constraint(
        connection,
        edge.__tablename__,
        'src_id',
        src_cls,
    )

    add_edge_fk_constraint(
        connection,
        edge.__tablename__,
        'dst_id',
        dst_cls,
    )


def drop_all_edge_fk_constraints(connection):
    """Deletes all fk constraints for all edges"""

    for edge in Edge.__subclasses__():
        drop_edge_fk_constraints(connection, edge)


def add_all_edge_fk_constraints(connection):
    """Creates all fk constraints for all edges"""

    for edge in Edge.__subclasses__():
        add_edge_fk_constraints(connection, edge)
