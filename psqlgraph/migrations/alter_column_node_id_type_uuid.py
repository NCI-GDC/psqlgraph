# -*- coding: utf-8 -*-
"""
migrations.alter_column_node_id_type_uuid
----------------------------------

Migrates up/down between states
A: {node}.node_id is of type Text
B: {node}.node_id is of type UUID
"""

from psqlgraph import Node, Edge

import logging

from edge_fk_constraint_utils import (
    add_all_edge_fk_constraints,
    drop_all_edge_fk_constraints,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def node_up(connection, node):
    logger.info('Migrating node {} up'.format(node))

    connection.execute("""
    ALTER TABLE {table} ALTER COLUMN node_id TYPE uuid USING node_id::uuid
    """.format(
        table=node.__tablename__,
    ))


def node_down(connection, node):
    logger.info('Migrating node {} down'.format(node))

    connection.execute("""
    ALTER TABLE {table} ALTER COLUMN node_id TYPE TEXT
    """.format(
        table=node.__tablename__,
    ))


def edge_up(connection, edge):
    logger.info('Migrating edge {} up'.format(edge))

    for column in 'src_id', 'dst_id':
        connection.execute("""
        ALTER TABLE {table}
        ALTER COLUMN {column} TYPE uuid USING {column}::uuid
        """.format(
            table=edge.__tablename__,
            column=column,
        ))


def edge_down(connection, edge):
    logger.info('Migrating edge {} down'.format(edge))

    for column in 'src_id', 'dst_id':
        connection.execute("""
        ALTER TABLE {table} ALTER COLUMN {column} TYPE text
        """.format(
            table=edge.__tablename__,
            column=column,
        ))


def up(connection):
    transaction = connection.begin()

    try:
        drop_all_edge_fk_constraints(connection)
        [node_up(connection, cls) for cls in Node.__subclasses__()]
        [edge_up(connection, cls) for cls in Edge.__subclasses__()]
        add_all_edge_fk_constraints(connection)
        transaction.commit()

    except Exception as e:
        print(e)
        transaction.rollback()
        raise
