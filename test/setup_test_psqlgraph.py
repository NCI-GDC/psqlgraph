"""
This is a one-time use script to set up a fresh install of Postgres 9.4
Needs to be run as the postgres user.
"""

import argparse
import logging
from test import models

from sqlalchemy import create_engine

from psqlgraph import PsqlGraphDriver, create_all


def try_drop_test_data(user, database, root_user="postgres", host=""):

    print("Dropping old test data")

    engine = create_engine(f"postgresql://{root_user}@{host}/postgres")

    conn = engine.connect()
    conn.execute("commit")

    try:
        create_stmt = f'DROP DATABASE "{database}"'
        conn.execute(create_stmt)
    except Exception as msg:
        logging.warning("Unable to drop test data:" + str(msg))

    try:
        user_stmt = f"DROP USER {user}"
        conn.execute(user_stmt)
    except Exception as msg:
        logging.warning("Unable to drop test data:" + str(msg))

    conn.close()


def setup_database(user, password, database, root_user="postgres", host=""):
    """
    setup the user and database
    """
    print("Setting up test database")

    try_drop_test_data(user, database)

    engine = create_engine(f"postgresql://{root_user}@{host}/postgres")
    conn = engine.connect()
    conn.execute("commit")

    create_stmt = f'CREATE DATABASE "{database}"'
    conn.execute(create_stmt)

    try:
        user_stmt = "CREATE USER {user} WITH PASSWORD '{password}'".format(
            user=user, password=password
        )
        conn.execute(user_stmt)

        perm_stmt = "GRANT ALL PRIVILEGES ON DATABASE {database} to {password}" "".format(
            database=database, password=password
        )
        conn.execute(perm_stmt)
        conn.execute("commit")
    except Exception as msg:
        logging.warning("Unable to add user:" + str(msg))
    conn.close()


def create_tables(host, user, password, database):
    """
    create a table
    """
    print("Creating tables in test database")

    driver = PsqlGraphDriver(host, user, password, database)
    create_all(driver.engine)


def create_indexes(host, user, password, database):
    """
    create a table
    """
    return


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--host", type=str, action="store", default="localhost", help="psql-server host"
    )
    parser.add_argument("--user", type=str, action="store", default="test", help="psql test user")
    parser.add_argument(
        "--password",
        type=str,
        action="store",
        default="test",
        help="psql test password",
    )
    parser.add_argument(
        "--database",
        type=str,
        action="store",
        default="automated_test",
        help="psql test database",
    )

    args = parser.parse_args()
    setup_database(args.user, args.password, args.database)
    create_tables(args.host, args.user, args.password, args.database)
    create_indexes(args.host, args.user, args.password, args.database)
