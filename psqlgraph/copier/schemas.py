# -*- coding: utf-8 -*-
"""
psqlgraph.copier.schemas
----------------------------------

Functionality to INSERT into schema from another schema in the same
database.

"""

from functools32 import lru_cache
from sqlalchemy.sql import table as sql_table, select, text
from sqlalchemy.orm import aliased
from sqlalchemy import literal, exists
from copy import deepcopy
from psqlgraph import Node, Edge
from sqlalchemy.dialects import postgresql

import json


NODE_UPSERT = (
    # Ok, this line is here because upserts are basically impossible
    # without serialization, built-ins, or locks.  Mode `share` was
    # chosen as the lowest mode that protects against concurrent data
    # changes:
    "Lock table {dst_table} in share mode;"

    # select the new values from `source`, which should be a
    # sqlalchemy.orm.Query, that will be comiled into this statement
    "With src_table ({columns}) as ({source}),"

    # This is the CTE that
    #   1. will perform our updates and
    #   2. tell us what updates failed so we can insert them
    # {set_props} must be a string that sets the columns in the update
    # e.g. _props = <src_table>._props
    """
    upsert as (
        update {dst_table} dst_table set
            {set_props}
        from src_table
        where dst_table.node_id = src_table.node_id
        returning dst_table.*
    )
    """

    # Perform the insert on things that didn't exist to update
    """
    Insert into {dst_table} ( {columns} )
    Select {columns} from src_table where not exists (
        select 1 from upsert upsert_cte
        where upsert_cte.node_id = src_table.node_id
    )
    """
)


# For comments on what this SQL does, see above, as it is very
# similar with different conditionals that are specific to edges
EDGE_UPSERT = """
    Lock table {dst_table} in share mode;

    With src_table ({columns}) as (
        {source}

        -- the following inner joins filter the given selection to
        -- include only those edges that satisfy the foreign key
        -- constraints for `src_id` and `dst_id`
        inner join {dst_src_node_table} src on {src_table}.src_id = src.node_id
        inner join {dst_dst_node_table} dst on {src_table}.dst_id = dst.node_id
    ),

    upsert as (
        update {dst_table} dst_table set
            {set_props}
        from src_table
        where dst_table.src_id = src_table.src_id and
              dst_table.dst_id = src_table.dst_id
        returning dst_table.*
    )

    Insert into {dst_table} ({columns})
    Select {columns} from src_table where not exists (
        select 1 from upsert upsert_cte
        where upsert_cte.src_id = src_table.src_id and
              upsert_cte.dst_id = src_table.dst_id
    )
"""



@lru_cache(maxsize=int(1e4))
def copy_sql_table(table, schema=None):
    clone = sql_table(table.name, *[c.copy() for c in table.columns])
    clone.schema = schema
    return clone


def full_tablename(schema, table):
    """Returns schema qualified tablename"""

    return '%s.%s' % (schema, table.name)


def sanitize_params(params):
    """Coerce params to something postgres can understand (mainly for JSONB)"""

    sanitized = {}

    for key, value in params.iteritems():
        if isinstance(value, dict):
            sanitized[key] = json.dumps(value)
        else:
            sanitized[key] = value

    return sanitized


def column_names_str(table):
    """Returns SQL specifier for column names"""

    return ', '.join(
        column.name
        for column in table.columns
    )


def column_setter_str(table):
    """Returns SQL specifier for column names,

    e.g. _props = <src_table>._props

    """

    return ',\n\t\t'.join(
        '{0} = src_table.{0}'.format(column.name)
        for column in table.columns
    )



def copy_node_baseclass(source_driver, src_schema, dst_schema, query):
    """Copy all node classes from given Node query to
    :param:`target_schema`

    """

    if not query.entity() is Node:
        raise TypeError("Type %s isn't Node" % query.entity())

    for cls in Node.get_subclasses():
        query = query.with_entities(cls)
        copy_nodes(source_driver, src_schema, dst_schema, query)


def copy_edge_baseclass(source_driver, src_schema, dst_schema, query):
    """Copy all edge classes from given Edge query to
    :param:`target_schema`

    """

    if not query.entity() is Edge:
        raise TypeError("Type %s isn't Edge" % query.entity())

    for cls in Edge.get_subclasses():
        query = query.with_entities(cls)
        copy_edges(source_driver, src_schema, dst_schema, query)


def copy_nodes(source_driver, src_schema, dst_schema, query):
    """Copy node class from given query to :param:`target_schema`

    note: Sorry, I would have use a writable CTE here with SQLAlchemy,
    but update.cte() doesn't exist for sqlalchmey<1.1 so SQL it is...

    """

    entity_type = query.entity()

    if not issubclass(entity_type, Node):
        raise TypeError("Type %s isn't subclass of Node" % entity_type)

    if entity_type is Node:
        return copy_node_baseclass(source_driver, src_schema, dst_schema, query)

    src_table = copy_sql_table(entity_type.__table__, src_schema)
    dst_table = copy_sql_table(entity_type.__table__, dst_schema)

    upsert = NODE_UPSERT.format(
        source=unicode(query),
        src_table=full_tablename(src_schema, src_table),
        dst_table=full_tablename(dst_schema, dst_table),
        columns=column_names_str(dst_table),
        set_props=column_setter_str(dst_table)
    )

    params = query.statement.compile(dialect=postgresql.dialect()).params
    with source_driver.session_scope() as session:
        session.execute(text(upsert), params=sanitize_params(params))


def copy_edges(source_driver, src_schema, dst_schema, query):
    """Copy edge class from given query to :param:`target_schema`

    ..note: see apology in :func:`copy_nodes()`

    ..warning:
        The upsert performed in this function will skip edges that
        don't meet the foreign key constraints on ``src_id`` and
        ``dst_id``

    """

    entity_type = query.entity()

    if not issubclass(entity_type, Edge):
        raise TypeError("Type %s isn't subclass of Edge" % entity_type)

    if entity_type is Edge:
        return copy_edge_baseclass(source_driver, src_schema, dst_schema, query)

    # These are the edge tables in the source and dest schemas
    src_schema_edge_table = copy_sql_table(entity_type.__table__, src_schema)
    dst_schema_edge_table = copy_sql_table(entity_type.__table__, dst_schema)

    # These are the src and dst node classes of the edge in the dst schema
    node_src_cls = Node.get_subclass_named(entity_type.__src_class__)
    node_dst_cls = Node.get_subclass_named(entity_type.__dst_class__)

    # These are the src and dst node tables of the edge in the dst schema
    dst_schema_src_table = node_src_cls.__table__
    dst_schema_dst_table = node_dst_cls.__table__

    upsert = EDGE_UPSERT.format(
        source=unicode(query),
        src_table=full_tablename(src_schema, src_schema_edge_table),
        dst_table=full_tablename(dst_schema, dst_schema_edge_table),
        dst_src_node_table=full_tablename(dst_schema, dst_schema_src_table),
        dst_dst_node_table=full_tablename(dst_schema, dst_schema_dst_table),
        columns=column_names_str(dst_schema_edge_table),
        set_props=column_setter_str(dst_schema_edge_table)
    )

    params = query.statement.compile(dialect=postgresql.dialect()).params
    with source_driver.session_scope() as session:
        session.execute(text(upsert), params=sanitize_params(params))
