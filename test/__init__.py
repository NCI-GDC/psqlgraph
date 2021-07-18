import logging
import unittest

import pytest

from psqlgraph import Edge, Node


class PsqlgraphBaseTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(PsqlgraphBaseTest, self).__init__(*args, **kwargs)
        self.REPEAT_COUNT = 20
        self.logger = logging.getLogger(__name__)

    @pytest.fixture(autouse=True)
    def init(self, pg_driver, pg_conf):
        self.pg_conf = pg_conf
        self.g = pg_driver

    def setUp(self):
        self._clear_tables()

    def tearDown(self):
        self._clear_tables()

    def _clear_tables(self):
        conn = self.g.engine.connect()
        conn.execute("commit")
        for table in Node().get_subclass_table_names():
            if table != Node.__tablename__:
                conn.execute("delete from {}".format(table))
        for table in Edge.get_subclass_table_names():
            if table != Edge.__tablename__:
                conn.execute("delete from {}".format(table))
        conn.execute("delete from _voided_nodes")
        conn.execute("delete from _voided_edges")
        conn.close()
