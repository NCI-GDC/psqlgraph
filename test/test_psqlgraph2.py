import uuid
import unittest
import logging
from psqlgraph import PsqlGraphDriver
from psqlgraph import Node

host = 'localhost'
user = 'test'
password = 'test'
database = 'automated_test'
g = PsqlGraphDriver(host, user, password, database)


logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

# We have to import models here, even if we don't use them
from models import Test, Foo, Edge1, Edge2


def _props(target, updates):
    return Test().property_template(updates)


class TestPsqlGraphDriver(unittest.TestCase):

    def setUp(self):
        self.nid = str(uuid.uuid4())
        self._clear_tables()
        self.node = Test(self.nid)
        with g.session_scope() as session:
            session.add(self.node)

    def tearDown(self):
        g.engine.dispose()

    def _clear_tables(self):
        conn = g.engine.connect()
        conn.execute('commit')
        for table in Node.get_subclass_table_names():
            if table != Node.__tablename__:
                conn.execute('delete from {}'.format(table))
        conn.execute('delete from {}'.format('_voided_nodes'))
        conn.close()

    def test_property_set(self):
        new = {'key1': 'first property'}
        with g.session_scope() as session:
            self.node['key1'] = new['key1']
            session.merge(self.node)
        with g.session_scope() as session:
            expected = _props(Test, new)
            self.assertEqual(
                g.nodes().ids(self.nid).one().properties, expected)

    def test_property_update(self):
        new = {'key1': 'first property'}
        with g.session_scope() as session:
            self.node.properties.update(new)
            session.merge(self.node)
            session.commit()
        with g.session_scope() as session:
            expected = _props(Test, new)
            self.assertEqual(
                g.nodes().ids(self.nid).one().properties, expected)

    def test_attribute_set(self):
        new = {'key1': 'first property'}
        with g.session_scope() as session:
            self.node.properties.update(new)
            self.node.key1 = 'first property'
            session.merge(self.node)
        with g.session_scope() as session:
            expected = _props(Test, new)
            self.assertEqual(
                g.nodes().ids(self.nid).one().properties, expected)

    def test_set_properties(self):
        new = {'key1': 'first property'}
        self.node.properties = new
        with g.session_scope() as session:
            session.merge(self.node)
        with g.session_scope() as session:
            expected = _props(Test, new)
            self.assertEqual(
                g.nodes().ids(self.nid).one().properties, expected)

    def test_validate_property(self):
        node = Test(str(uuid.uuid4()))
        node._set_property('key1', 1)  # Should not raise
        node.key1 = 'test'
        node['key1'] = 'test'
        node.properties['key1'] = 'test'
        node._set_property('key1', 'test')
        node.properties.update({'key1': 'test'})
        with self.assertRaises(AssertionError):
            node.key1 = 1
        with self.assertRaises(AssertionError):
            node['key1'] = 1
        with self.assertRaises(AssertionError):
            node.properties['key1'] = 1
        with self.assertRaises(AssertionError):
            node.properties.update({'key1': 1})

    def test_set_query_result_properties(self):
        new = {'key1': 'first property'}
        with g.session_scope() as session:
            queried = g.nodes().ids(self.nid).one()
            queried.properties = new
            session.merge(queried)
        with g.session_scope() as session:
            expected = _props(Test, new)
            self.assertEqual(
                g.nodes().ids(self.nid).one().properties, expected)

    def test_update_query_result_properties(self):
        new = {'key1': 'first property', 'key2': 'first pass'}
        self.node.key1 = new['key2']
        with g.session_scope() as session:
            session.merge(self.node)
        with g.session_scope() as session:
            queried = g.nodes().ids(self.nid).one()
            queried.properties.update(new)
            session.merge(queried)
        with g.session_scope() as session:
            expected = _props(Test, new)
            self.assertEqual(
                g.nodes().ids(self.nid).one().properties, expected)

    def test_set_query_result_attribute(self):
        new = {'key1': 'first property', 'key2': 'first pass'}
        node = Test(self.nid)
        node.key2 = new['key2']
        with g.session_scope() as session:
            session.merge(node)
        with g.session_scope() as session:
            queried = g.nodes().ids(self.nid).one()
            queried.key1 = new['key1']
            session.merge(queried)
        with g.session_scope() as session:
            expected = _props(Test, new)
            self.assertEqual(
                g.nodes().ids(self.nid).one().properties, expected)

    def test_add_edge(self):
        src_id, dst_id = str(uuid.uuid4()), str(uuid.uuid4())
        src, dst = Test(src_id), Test(dst_id)
        edge = Edge1(src_id, dst_id)
        with g.session_scope() as session:
            session.add_all([src, dst])
            session.flush()
            session.add(edge)
            g.edges().filter(Edge1.src_id == src_id).one()

    def test_edge_attributes(self):
        foo = Foo('foonode')
        edge = Edge2(self.nid, 'foonode')
        with g.session_scope() as s:
            s.add_all((edge, foo))
        with g.session_scope() as s:
            n = g.nodes().ids(self.nid).one()
            self.assertEqual(n.foos[0].node_id, 'foonode')
