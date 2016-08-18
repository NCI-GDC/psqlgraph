"""
This is a one-time use script to set up a fresh install of Postgres 9.4
Needs to be run as the postgres user.
"""

from sqlalchemy import create_engine
from models import *

import conftest
import logging

from psqlgraph import (
    PsqlGraphDriver,
    create_all,
    Node,
    Edge,
    VoidedNode,
    VoidedEdge,
)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("setup")


def try_execute(conn, statement):
    try:
        logger.info('TRY EXECUTE: "%s"' % statement)
        conn.execute(statement)
        conn.execute('commit')
    except Exception as msg:
        logger.warn('Failed to execute: "%s": %s', statement, msg)



def try_drop_test_data(user, database, root_user='postgres', host=''):
    logger.info('Dropping old test data')

    engine = create_engine("postgres://{user}@{host}/postgres".format(
        user=root_user, host=host))

    conn = engine.connect()
    conn.execute("commit")
    try_execute(conn, 'Drop database "{database}"'.format(database=database))
    try_execute(conn, 'Drop user {user}'.format(user=user))
    conn.close()


def setup_database(schemas, user, password, database,
                   root_user='postgres', host=''):
    """Setup the user and database"""

    logger.info('Setting up test database')

    try_drop_test_data(user, database)

    engine = create_engine("postgres://{user}@/postgres".format(user=root_user))
    conn = engine.connect()
    conn.execute("commit")

    # Create database
    try_execute(conn, 'Create database "{database}"'.format(database=database))

    # Create user
    try_execute(conn, "Create user {user} with password '{password}'"
                .format(user=user, password=password))

    # Create schemas
    engine = create_engine("postgres://{user}@/{database}"
                           .format(user=root_user, database=database))
    for schema in schemas:
        engine.execute("Create schema if not exists %s" % schema)
        conn.execute('Set search_path to %s' % schema)

    conn.close()


def get_tables():
    classes =(
        Node.__subclasses__()
        + Edge.__subclasses__()
        + [VoidedNode, VoidedEdge]
    )

    return [cls.__tablename__ for cls in classes] + [
        '_voided_edges_key_seq',
        '_voided_nodes_key_seq',
    ]


def grant_all(conn, user, schema):
    try_execute(conn, 'set session search_path to %s; commit;' % schema)
    try_execute(conn, 'Grant usage on schema %s to %s' % (schema, user))

    for table in get_tables():
        try_execute(conn, 'Grant all on {table} to {user}'
                    .format(user=user, table=table))


def create_schema_tables(conn, schema):
    try_execute(conn, 'set session search_path to %s' % schema)
    create_all(conn)


def create_tables(schemas, **pg_kwargs):
    """Create all tables"""

    user, pg_kwargs['user'] = pg_kwargs['user'], 'postgres'

    for schema in schemas:
        logger.info('Creating tables in test database, schema: %s', schema)
        driver = PsqlGraphDriver(**pg_kwargs)
        conn = driver.engine.connect()
        create_schema_tables(conn, schema)
        grant_all(conn, user, schema)
        conn.close()


def destroy_and_setup_database():
    schemas = ['public', 'schema2']
    setup_database(schemas, **conftest.pg_config())
    create_tables(schemas, **conftest.pg_config())


if __name__ == '__main__':
    destroy_and_setup_database()
