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
from models import Test, Foo, Edge1


def _props(target, updates):
    return Test().property_template(updates)


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
        conn.execute('delete from {}'.format('_voided_nodes'))
        conn.close()

    def test_property_set(self):
        nid = str(uuid.uuid4())
        node = Test(nid)
        new = {'key1': 'first property'}
        with g.session_scope() as session:
            node['key1'] = new['key1']
            session.merge(node)
        with g.session_scope() as session:
            expected = _props(Test, new)
            self.assertEqual(g.nodes().ids(nid).one().properties, expected)

    def test_property_update(self):
        nid = str(uuid.uuid4())
        node = Test(nid)
        new = {'key1': 'first property'}
        with g.session_scope() as session:
            session.merge(node)
            session.commit()
            node.properties.update(new)
            session.merge(node)
            session.commit()
        with g.session_scope() as session:
            expected = _props(Test, new)
            self.assertEqual(g.nodes().ids(nid).one().properties, expected)

    def test_attribute_set(self):
        nid = str(uuid.uuid4())
        node = Test(nid)
        with g.session_scope() as session:
            session.merge(node)
        new = {'key1': 'first property'}
        with g.session_scope() as session:
            node.properties.update(new)
            node.key1 = 'first property'
            session.merge(node)
        with g.session_scope() as session:
            expected = _props(Test, new)
            self.assertEqual(g.nodes().ids(nid).one().properties, expected)

    def test_set_properties(self):
        nid = str(uuid.uuid4())
        node = Test(nid)
        new = {'key1': 'first property'}
        node.properties = new
        with g.session_scope() as session:
            session.merge(node)
        with g.session_scope() as session:
            expected = _props(Test, new)
            self.assertEqual(g.nodes().ids(nid).one().properties, expected)

    def test_set_query_result_properties(self):
        nid = str(uuid.uuid4())
        # node = Test(nid, {'key2': 'first pass'})
        new = {'key1': 'first property', 'key2': 'first pass'}
        node = Test(nid, dict(key2=new['key2']))
        with g.session_scope() as session:
            session.merge(node)
        with g.session_scope() as session:
            queried = g.nodes().ids(nid).one()
            queried.properties = new
            session.merge(queried)
        with g.session_scope() as session:
            expected = _props(Test, new)
            self.assertEqual(g.nodes().ids(nid).one().properties, expected)

    def test_set_query_result_attribute(self):
        nid = str(uuid.uuid4())
        new = {'key1': 'first property', 'key2': 'first pass'}
        node = Test(nid, dict(key2=new['key2']))
        with g.session_scope() as session:
            session.merge(node)
        with g.session_scope() as session:
            queried = g.nodes().ids(nid).one()
            queried.key1 = new['key1']
            session.merge(queried)
        with g.session_scope() as session:
            expected = _props(Test, new)
            self.assertEqual(g.nodes().ids(nid).one().properties, expected)

    def test_add_edge(self):
        src_id, dst_id = str(uuid.uuid4()), str(uuid.uuid4())
        src, dst = Test(src_id), Test(dst_id)
        edge = Edge1(src_id, dst_id)
        with g.session_scope() as session:
            session.add_all([src, dst])
            session.flush()
            session.add(edge)
            g.edges().filter(Edge1.src_id == src_id).one()
