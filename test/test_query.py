import unittest
import logging
import uuid
from psqlgraph import Node, Edge, PsqlGraphDriver
from psqlgraph import PolyNode, PolyEdge

host = 'localhost'
user = 'test'
password = 'test'
database = 'automated_test'
g = PsqlGraphDriver(host, user, password, database)

logging.basicConfig(level=logging.INFO)

from models import Test, Foo, Edge1, Edge2, Edge3


class TestPsqlGraphDriver(unittest.TestCase):

    def setUp(self):
        self.logger = logging.getLogger(__name__)
        self.g = g
        self.parent_id = str(uuid.uuid4())
        self.g.node_insert(PolyNode(self.parent_id, 'test'))
        self._create_subtree(self.parent_id)
        self.lone_id = str(uuid.uuid4())
        self.g.node_insert(PolyNode(self.lone_id, 'test'))

    def _create_subtree(self, parent_id, level=0):
        for i in range(4):
            node_id = str(uuid.uuid4())
            self.g.node_merge(
                node_id=node_id, label='test',
                properties={'key2': i, 'key3': None})
            self.g.edge_insert(PolyEdge(
                src_id=parent_id, dst_id=node_id, label='edge1'))
            if level < 2:
                self._create_subtree(node_id, level+1)

    def tearDown(self):
        self.g.engine.dispose()
        self._clear_tables()

    def _clear_tables(self):
        conn = g.engine.connect()
        conn.execute('commit')
        for table in Node().get_subclass_table_names():
            if table != Node.__tablename__:
                conn.execute('delete from {}'.format(table))
        for table in Edge().get_subclass_table_names():
            if table != Edge.__tablename__:
                conn.execute('delete from {}'.format(table))
        conn.execute('delete from _voided_nodes')
        conn.execute('delete from _voided_edges')
        conn.close()

    def test_ids(self):
        with self.g.session_scope():
            self.assertTrue(self.g.nodes().ids(self.lone_id).one()
                            .node_id == self.lone_id)

    def test_not_ids(self):
        with self.g.session_scope():
            for n in self.g.nodes().not_ids(self.lone_id).all():
                self.assertNotEqual(n.node_id, self.lone_id)

    def test_labels(self):
        with self.g.session_scope():
            for i in range(3):
                label = 'test'
                ns = self.g.nodes().labels(label).all()
                self.assertTrue(ns != [])
                for n in ns:
                    self.assertTrue(n.label == label)

    def test_props(self):
        with self.g.session_scope():
            for i in range(3):
                ns = self.g.nodes().props(key2=i).all()
                self.assertNotEqual(ns, [])
                for n in ns:
                    self.assertEqual(n['key2'], i)
                    self.assertEqual(n['key3'], None)

    def test_not_props(self):
        with self.g.session_scope():
            for i in range(3):
                ns = self.g.nodes().not_props({'key2': i, 'key3': None})\
                                   .all()
                self.assertNotEqual(ns, [])
                for n in ns:
                    self.assertNotEqual(n['key2'], i)
                    self.assertEqual(n['key3'], None)

    def test_path(self):
        with self.g.session_scope():
            self.assertEqual(self.g.nodes(Test)
                             .ids(self.parent_id)
                             .path('tests')
                             .props(key2=1)
                             .count(), 1)
            self.assertEqual(self.g.nodes(Test)
                             .ids(self.parent_id)
                             .path('tests.tests')
                             .props(key2=1)
                             .count(), 4)
            self.assertEqual(len(self.g.nodes(Test)
                                 .path('tests.tests')
                                 .props(key2=1)
                                 .all()), 21)
            self.assertEqual(len(self.g.nodes(Test)
                                 .path('tests.tests.tests')
                                 .props(key2=1)
                                 .all()), 21)
