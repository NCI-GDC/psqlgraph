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
            foo_id = str(uuid.uuid4())
            self.g.node_merge(
                node_id=node_id, label='test',
                properties={'key2': i, 'key3': None})
            self.g.node_merge(
                node_id=foo_id, label='foo',
                properties={'bar': i})
            self.g.edge_insert(PolyEdge(
                src_id=parent_id, dst_id=node_id, label='edge1'))
            self.g.edge_insert(PolyEdge(
                src_id=parent_id, dst_id=foo_id, label='test_edge_2'))
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
                ns = self.g.nodes(Test).not_props({'key2': i, 'key3': None})\
                                       .all()
                self.assertNotEqual(ns, [])
                for n in ns:
                    self.assertNotEqual(n['key2'], i)
                    self.assertEqual(n['key3'], None)

    def test_path(self):
        with self.g.session_scope():
            self.assertEqual(self.g.nodes(Test)
                             .ids(self.parent_id)
                             .path('foos')
                             .props(bar=1)
                             .count(), 1)

    def test_subq_path_no_filter(self):
        with self.g.session_scope():
            self.assertEqual(self.g.nodes(Test)
                             .ids(self.parent_id)
                             .subq_path('foos')
                             .count(), 4)

    def test_subq_path_single_filter(self):
        with self.g.session_scope():
            self.assertEqual(self.g.nodes(Test)
                             .ids(self.parent_id)
                             .subq_path('foos', lambda q: q.props(bar=1))
                             .count(), 1)

    def test_subq_path_single_filter_negative(self):
        with self.g.session_scope():
            self.assertEqual(self.g.nodes(Test)
                             .ids(self.parent_id)
                             .subq_path('foos', lambda q: q.props(bar=-1))
                             .count(), 0)

    def test_subq_path_multi_filters(self):
        with self.g.session_scope():
            self.assertEqual(
                self.g.nodes(Foo)
                .subq_path('tests.foos.tests.foos', [
                    lambda q: q.props(bar=1),
                    lambda q: q.ids(self.parent_id),
                    lambda q: q.props(bar=3)
                ]).count(), 4)

    def test_subq_path_multi_filters_negative(self):
        with self.g.session_scope():
            self.assertEqual(
                self.g.nodes(Foo)
                .subq_path('tests.foos.tests.foos', [
                    lambda q: q.props(bar=-1),
                    lambda q: q.ids(self.parent_id),
                    lambda q: q.props(bar=3)
                ]).count(), 0)

    def test_subq_without_path_no_filter(self):
        with self.g.session_scope():
            self.assertEqual(self.g.nodes(Foo)
                             .subq_without_path('tests')
                             .count(), 0)

    def test_subq_without_path_filter(self):
        with self.g.session_scope():
            self.assertEqual(self.g.nodes(Foo)
                             .subq_without_path(
                                 'tests',
                                 [lambda q: q.ids('test')])
                             .count(), self.g.nodes(Foo).count())
