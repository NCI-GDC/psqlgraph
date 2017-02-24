"""
This is a one-time use script to set up a fresh install of Postgres 9.4
Needs to be run as the postgres user.
"""

from sqlalchemy import create_engine
from psqlgraph import create_all
from psqlgraph import PsqlGraphDriver
from models import *

import conftest
import logging


def try_drop_test_data(user, database, root_user='postgres', host=''):

    print('Dropping old test data')

    engine = create_engine("postgres://{user}@{host}/postgres".format(
        user=root_user, host=host))

    conn = engine.connect()
    conn.execute("commit")

    try:
        create_stmt = 'DROP DATABASE "{database}"'.format(database=database)
        conn.execute(create_stmt)
    except Exception, msg:
        logging.warn("Unable to drop test data:" + str(msg))

    try:
        user_stmt = "DROP USER {user}".format(user=user)
        conn.execute(user_stmt)
    except Exception, msg:
        logging.warn("Unable to drop test data:" + str(msg))

    conn.close()


def setup_database(user, password, database, root_user='postgres', host=''):
    """
    setup the user and database
    """
    print('Setting up test database')

    try_drop_test_data(user, database)

    engine = create_engine("postgres://{user}@{host}/postgres".format(
        user=root_user, host=host))
    conn = engine.connect()
    conn.execute("commit")

    create_stmt = 'CREATE DATABASE "{database}"'.format(database=database)
    conn.execute(create_stmt)

    try:
        user_stmt = "CREATE USER {user} WITH PASSWORD '{password}'".format(
            user=user, password=password)
        conn.execute(user_stmt)

        perm_stmt = 'GRANT ALL PRIVILEGES ON DATABASE {database} to {password}'\
                    ''.format(database=database, password=password)
        conn.execute(perm_stmt)
        conn.execute("commit")
    except Exception, msg:
        logging.warn("Unable to add user:" + str(msg))
    conn.close()


def create_tables(host, user, password, database):
    """
    create a table
    """
    print('Creating tables in test database')

    driver = PsqlGraphDriver(host, user, password, database)
    create_all(driver.engine)


def destroy_and_setup_database(host, user, password, database):
    setup_database(user, password, database)
    create_tables(host, user, password, database)


def setup_databases():
    destroy_and_setup_database(
        conftest.PG_SOURCE_HOST,
        conftest.PG_SOURCE_USER,
        conftest.PG_SOURCE_PASSWORD,
        conftest.PG_SOURCE_DATABASE)

    destroy_and_setup_database(
        conftest.PG_DESTINATION_HOST,
        conftest.PG_DESTINATION_USER,
        conftest.PG_DESTINATION_PASSWORD,
        conftest.PG_DESTINATION_DATABASE)


if __name__ == '__main__':
    setup_databases()
