import logging
import unittest

import pytest

import psqlgraph


class PsqlgraphBaseTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.REPEAT_COUNT = 20
        self.logger = logging.getLogger(__name__)

    @pytest.fixture(autouse=True)
    def init(self, pg_driver: psqlgraph.PsqlGraphDriver, pg_conf: dict):
        self.pg_conf = pg_conf
        self.g = pg_driver

    def setUp(self):
        self._clear_tables()

    def tearDown(self):
        self._clear_tables()

    def _clear_tables(self):
        psqlgraph.drop_all(self.g.engine)
        psqlgraph.create_all(self.g.engine)
