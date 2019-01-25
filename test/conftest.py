import pytest

import psqlgraph


@pytest.fixture(scope="session")
def pg_conf():
    return {
        'host': "localhost",
        'user': "test",
        'password': "test",
        'database': "automated_test"
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
