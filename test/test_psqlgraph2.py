import uuid
import unittest
import logging
import psqlgraph
import random
from psqlgraph import PsqlGraphDriver
from psqlgraph.node import PsqlNode, Node, PolyNode, VoidedNode, sanitize
from multiprocessing import Process
from sqlalchemy.exc import IntegrityError
from psqlgraph.exc import ValidationError, EdgeCreationError

from datetime import datetime
from copy import deepcopy, copy

host = 'localhost'
user = 'test'
password = 'test'
database = 'automated_test'
g = PsqlGraphDriver(host, user, password, database)


logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

# We have to import models here, even if we don't use them
from models import TestNode, FooNode, FooEdge

def _props(cls, updates):
    props = {k: v for k, v in cls().properties.items()}
    props.update(updates)
    return props


class TestPsqlGraphDriver(unittest.TestCase):

    def setUp(self):
        self._clear_tables()

    def tearDown(self):
        g.engine.dispose()

    def _clear_tables(self):
        conn = g.engine.connect()
        conn.execute('commit')
        for table in Node.get_subclass_table_names():
            if table != Node.__tablename__:
                conn.execute('delete from {}'.format(table))
        conn.execute('delete from {}'.format('_nodes'))
        conn.execute('delete from {}'.format('_voided_nodes'))
        conn.close()

    def test_property_set(self):
        nid = str(uuid.uuid4())
        node = TestNode(nid)
        with g.session_scope() as session:
            session.merge(node)
        new = {'key1': 'first property'}
        with g.session_scope() as session:
            node['key1'] = new['key1']
            session.merge(node)
        with g.session_scope() as session:
            expected = _props(TestNode, new)
            self.assertEqual(g.nodes().ids(nid).one().properties, expected)

    def test_property_update(self):
        nid = str(uuid.uuid4())
        node = TestNode(nid)
        with g.session_scope() as session:
            session.merge(node)
        new = {'key1': 'first property'}
        with g.session_scope() as session:
            node.properties.update(new)
            session.merge(node)
        with g.session_scope() as session:
            expected = _props(TestNode, new)
            self.assertEqual(g.nodes().ids(nid).one().properties, expected)
