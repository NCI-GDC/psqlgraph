import os
import uuid
from test import models

import pytest
from _pytest import fixtures
from testcontainers import postgres

import psqlgraph


@pytest.fixture(scope="session")
def pg_conf():
    return {
        "host": os.getenv("PG_HOST", "localhost"),
        "user": os.getenv("PG_USER", "test"),
        "password": os.getenv("PG_PASS", "test"),
        "database": os.getenv("PG_NAME", "automated_test"),
    }


@pytest.fixture(scope="session")
def pg_driver(request, pg_conf):
    pg_graph_driver = psqlgraph.PsqlGraphDriver(**pg_conf)

    def drop_all():
        psqlgraph.base.ORMBase.metadata.drop_all(pg_graph_driver.engine)
        psqlgraph.base.VoidedBase.metadata.drop_all(pg_graph_driver.engine)

    request.addfinalizer(drop_all)

    drop_all()

    psqlgraph.create_all(pg_graph_driver.engine)

    return pg_graph_driver


SAMPLES = [
    dict(
        bar="bar1",
        baz="allowed_1",
        fobble=25,
        studies=["P1", "P2"],
        ages=[23, 45],
        node_id=str(uuid.uuid4()),
    ),
    dict(
        bar="bar2",
        baz="allowed_1",
        fobble=25,
        studies=["C1", "P2"],
        ages=[29, 45],
        node_id=str(uuid.uuid4()),
    ),
    dict(
        bar="bar3",
        baz="allowed_2",
        fobble=25,
        studies=["P2"],
        ages=[29],
        node_id=str(uuid.uuid4()),
    ),
]


@pytest.fixture()
def samples_with_array(pg_driver):
    nodes = []
    with pg_driver.session_scope() as s:
        for sample in SAMPLES:
            foo = models.Foo(**sample)
            s.add(foo)
            nodes.append(foo)
    yield nodes

    with pg_driver.session_scope():
        for node in nodes:
            n = pg_driver.nodes().get(node.node_id)
            if n:
                pg_driver.node_delete(node=n)


@pytest.fixture(scope="session", autouse=True)
def reset_database(request: fixtures.SubRequest) -> None:
    """Clear database migration at the end of tests."""

    if os.getenv("CI_COMMIT_REF_NAME"):
        return

    pg = postgres.PostgresContainer("postgres:15", driver="psycopg", dbname="automated_test")
    pg.start()

    os.environ["PG_HOST"] = f"{pg.get_container_host_ip()}:{pg.get_exposed_port(5432)}"
    request.addfinalizer(pg.stop)
