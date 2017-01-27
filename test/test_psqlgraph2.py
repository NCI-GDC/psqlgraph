import uuid
import unittest
import logging
from psqlgraph import PsqlGraphDriver, VoidedNode
from psqlgraph import Node
from psqlgraph.exc import ValidationError
from psqlgraph.exc import SessionClosedError
import socket
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

    def test_default_application_name(self):
        cmd = "select application_name from pg_stat_activity;"
        with g.session_scope() as s:
            s.merge(Test('a'))
            app_names = {r[0] for r in g.engine.execute(cmd)}
        self.assertIn(socket.gethostname(), app_names)

    def test_custom_application_name(self):
        cmd = "select application_name from pg_stat_activity;"
        custom_name = '_CUSTOM_NAME'
        g_ = PsqlGraphDriver(host, user, password, database,
                             application_name=custom_name)
        with g_.session_scope() as s:
            s.merge(Test('a'))
            app_names = {r[0] for r in g.engine.execute(cmd)}
        self.assertIn(custom_name, app_names)

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
            a = s.merge(a)
            s.commit()
            a.sysan['key1'] = value
            a = s.merge(a)
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

    def test_snapshot_on_delete(self):
        a = Test(str(uuid.uuid4()), key2=1)
        g.node_insert(a)
        with g.session_scope() as s:
            s.delete(a)
            self.assertEqual(a._history.one().properties['key2'], 1)

    def test_snapshot_sysan(self):
        a = Test(str(uuid.uuid4()), key2=1)
        with g.session_scope() as s:
            a = s.merge(a)
            s.commit()
            a.sysan['key'] = 1
            a = s.merge(a)
            s.commit()
            self.assertEqual(a._history.one().properties['key2'], 1)
            a.sysan['key'] = 3
            a = s.merge(a)

    def test_session_closing(self):
        with g.session_scope():
            nodes = g.nodes()
        with self.assertRaises(SessionClosedError):
            nodes.first()

    def test_sysan_sanitization(self):
        """It shouldn't be possible to set system annotations keys to
        anything but primitive values.

        """
        a = Test(str(uuid.uuid4()))
        with g.session_scope() as s:
            a = s.merge(a)
            with self.assertRaises(ValueError):
                a.sysan["foo"] = {"bar": "baz"}

    def test_session_timestamp(self):
        with g.session_scope() as s:
            self.assertIsNone(s._flush_timestamp)
            s.merge(Test(""))
            s.flush()
            self.assertIsNotNone(s._flush_timestamp)
        g.set_flush_timestamps = False
        with g.session_scope() as s:
            s.merge(Test(""))
            s.flush()
            self.assertIsNone(s._flush_timestamp)

    def test_custom_insert_hooks(self):
        """Test that all custom insert hooks are called."""

        self._clear_tables()
        node_id = 'test_insert'

        def add_key1(target, session, *args, **kwargs):
            target.key1 = 'value1'

        def add_key2(target, session, *args, **kwargs):
            target.key2 = 'value2'

        Test._session_hooks_before_insert = [
            add_key1,
            add_key2,
        ]

        try:
            with g.session_scope() as s:
                test = Test(node_id)
                s.merge(test)

            with g.session_scope():
                test = g.nodes(Test).ids(node_id).one()
                self.assertEqual(test.key1, 'value1')
                self.assertEqual(test.key2, 'value2')

        finally:
            Test._session_hooks_before_insert = []

    def test_custom_hooks_are_class_local(self):
        """Test that all custom hooks affect single classes."""

        self._clear_tables()
        node_id = 'test_locality'

        def bad_hook(target, session, *args, **kwargs):
            raise RuntimeError

        Test._session_hooks_before_insert = [
            bad_hook,
        ]

        try:
            with self.assertRaises(RuntimeError):
                with g.session_scope() as s:
                    test = Test(node_id)
                    s.merge(test)

            with g.session_scope() as s:
                foo = Foo(node_id)
                s.merge(foo)

            with g.session_scope():
                foo = g.nodes(Foo).ids(node_id).one()
                self.assertEqual(foo.bar, None)
                self.assertEqual(foo.baz, None)

        finally:
            Test._session_hooks_before_insert = []

    def test_custom_update_hooks(self):
        """Test that all custom insert hooks are called."""

        self._clear_tables()
        node_id = 'test_update'

        def add_key1(target, session, *args, **kwargs):
            target.key1 = 'value1'

        def add_key2(target, session, *args, **kwargs):
            target.key2 = 'value2'

        Test._session_hooks_before_update = [
            add_key1,
            add_key2,
        ]

        try:
            with g.session_scope() as s:
                assert not g.nodes(Test).ids(node_id).first()
                test = Test(node_id)
                s.merge(test)

            with g.session_scope():
                test = g.nodes(Test).ids(node_id).one()
                self.assertEqual(test.key1, None)
                self.assertEqual(test.key2, None)

            with g.session_scope() as s:
                test = Test(node_id)
                s.merge(test)

            with g.session_scope():
                test_ = g.nodes(Test).ids(node_id).one()
                self.assertEqual(test_.key1, 'value1')
                self.assertEqual(test_.key2, 'value2')

        finally:
            Test._session_hooks_before_update = []

    def test_custom_delete_hooks(self):
        """Test that all custom pre-delete hooks are called."""

        self._clear_tables()
        node_id = 'test_update'
        new_node_id1 = 'pre-delete-1'
        new_node_id2 = 'pre-delete-2'

        def add_new_node1(target, session, *args, **kwargs):
            session.merge(Test(new_node_id1))

        def add_new_node2(target, session, *args, **kwargs):
            session.merge(Test(new_node_id2))

        Test._session_hooks_before_delete = [
            add_new_node1,
            add_new_node2,
        ]

        try:
            with g.session_scope() as s:
                assert not g.nodes(Test).ids(node_id).first()
                test = Test(node_id)
                test = s.merge(test)
                s.commit()
                s.delete(test)

            with g.session_scope():
                g.nodes(Test).ids(new_node_id1).one()
                g.nodes(Test).ids(new_node_id2).one()
                self.assertFalse(g.nodes(Test).ids(node_id).scalar())

        finally:
            Test._session_hooks_before_delete = []

    def test_null_props_unset(self):
        self._clear_tables()

        with g.session_scope() as session:
            session.merge(Test('a'))
            session.merge(Test('b', key1='1', key2='2', key3='3'))

        with g.session_scope() as session:
            g.nodes(Test).null_props('key1').one()
            g.nodes(Test).null_props(['key1', 'key2']).one()
            g.nodes(Test).null_props('key1', 'key2').one()
            g.nodes(Test).null_props(['key1', 'key2'], 'key3').one()
            g.nodes(Test).null_props('key1').one()

    def test_null_props_set(self):
        self._clear_tables()

        with g.session_scope() as session:
            session.merge(Test('a', key1=None, key2=None, key3=None))
            session.merge(Test('b', key1='1', key2='2', key3='3'))

        with g.session_scope() as session:
            g.nodes(Test).null_props('key1').one()
            g.nodes(Test).null_props(['key1', 'key2']).one()
            g.nodes(Test).null_props('key1', 'key2').one()
            g.nodes(Test).null_props(['key1', 'key2'], 'key3').one()
            g.nodes(Test).null_props('key1').one()
