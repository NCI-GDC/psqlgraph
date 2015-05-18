import uuid
import unittest
import logging
from psqlgraph import PsqlGraphDriver
from psqlgraph import Node
from psqlgraph.exc import ValidationError
import sqlalchemy as sa

host = 'localhost'
user = 'test'
password = 'test'
database = 'automated_test'
g = PsqlGraphDriver(host, user, password, database)


logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

# We have to import models here, even if we don't use them
from models import Test, Foo, Edge1, Edge2, FooBar


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

    def test_property_merge(self):
        node = Test('a')
        node.key1 = 'first'
        node.key2 = 'first'
        with g.session_scope() as session:
            session.merge(node)
        node = Test('a')
        node.key1 = 'second'
        node.key1 = 'third'
        with g.session_scope() as session:
            session.merge(node)
        with g.session_scope() as session:
            node = g.nodes(Test).ids('a').one()
        self.assertEqual(node.key1, 'third')
        self.assertEqual(node.key2, 'first')

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
        with self.assertRaises(AssertionError):
            node.properties = {'key1': 1}

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

    def test_type_enum(self):
        with self.assertRaises(ValidationError):
            Foo().fobble = 'test'

    def test_validate_enum(self):
        n = Foo('foonode')
        n.baz = 'allowed_1'
        n.baz = 'allowed_2'
        with self.assertRaises(ValidationError):
            n.baz = 'not allowed'

    def test_association_proxy(self):
        a = Test('a')
        b = Foo('b')
        c = Test('c')
        a.foos.append(b)
        a.tests = [c]
        with g.session_scope() as s:
            a, b, c = map(s.merge, (a, b, c))
        with g.session_scope() as s:
            a = g.nodes(Test).ids('a').one()
            self.assertTrue(b in a.foos)
            self.assertEqual(a.tests, [c])

    def test_relationship_population_setter(self):
        a = Test('a')
        b = Foo('b')
        e = Edge2()
        e.src = a
        e.dst = b
        with g.session_scope() as s:
            a, b = map(s.merge, (a, b))
            e2 = s.query(Edge2).src('a').dst('b').one()
            e = a._Edge2_out[0]
            self.assertEqual(e2, e)

    def test_nonnull(self):
        a = FooBar('a')
        a.bar = None
        a.props['bar'] = None
        a.properties = {'bar': None}
        with self.assertRaises(AssertionError):
            with g.session_scope() as s:
                s.merge(a)
        a.bar = True
        with g.session_scope() as s:
            s.merge(a)

    def test_relationship_population_constructer(self):
        a = Test('a')
        b = Foo('b')
        e = Edge2(src=a, dst=b)
        with g.session_scope() as s:
            a, b = map(s.merge, (a, b))
            e2 = s.query(Edge2).src('a').dst('b').one()
            e = a._Edge2_out[0]
            self.assertEqual(e2, e)

    def test_cascade_delete(self):
        a = Test('a')
        b = Foo('b')
        a.foos = [b]
        with g.session_scope() as s:
            a, b = map(s.merge, (a, b))
        with g.session_scope() as s:
            s.delete(g.nodes(Test).ids('a').one())
        with g.session_scope() as s:
            self.assertIsNone(g.nodes(Test).ids('a').scalar())
            self.assertIsNone(g.edges(Edge2).src('a').dst('b').scalar())

    def test_set_sysan(self):
        a = Test('a')
        with g.session_scope() as s:
            a = s.merge(a)
        with g.session_scope() as s:
            a = g.nodes(Test).ids('a').one()
            a = s.merge(a)
            a.sysan['key1'] = True
            s.merge(a)
        with g.session_scope() as s:
            n = g.nodes(Test).ids('a').one()
            print n.sysan
            self.assertTrue(n.sysan['key1'])

    def test_unchanged_properties_snapshot(self):
        nid = str(uuid.uuid4())
        a = Test(nid)
        value = '++ VALUE ++'
        with g.session_scope() as s:
            a.key1 = value
            s.merge(a)
            s.commit()
            a = g.nodes(Test).ids(nid).one()
            a.key1 = value
            s.merge(a)
            s.commit()
            self.assertEqual(a._history.all(), [])

    def test_unchanged_sysan_snapshot(self):
        nid = str(uuid.uuid4())
        a = Test(nid)
        value = '++ VALUE ++'
        with g.session_scope() as s:
            a.sysan['key1'] = value
            s.merge(a)
            s.commit()
            a = g.nodes(Test).ids(nid).one()
            a.sysan['key1'] = value
            s.merge(a)
            s.commit()
            self.assertEqual(a._history.all(), [])

    def test_isolation_level(self):
        nid = str(uuid.uuid4())
        with g.session_scope() as s:
            s.merge(Test(nid))
        with self.assertRaises(sa.exc.OperationalError):
            with g.session_scope() as outer:
                a = outer.query(Test).filter(Test.node_id == nid).one()
                with g.session_scope(can_inherit=False) as inner:
                    b = inner.query(Test).filter(Test.node_id == nid).one()
                    b.sysan['inner'] = True
                    inner.merge(b)
                    inner.commit()
                a.sysan['outer'] = True
