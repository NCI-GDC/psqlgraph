import uuid
import logging
from psqlgraph import PsqlGraphDriver
from psqlgraph.exc import ValidationError
from psqlgraph.exc import SessionClosedError
import socket
import sqlalchemy as sa

# We have to import models here, even if we don't use them
from test import models, PsqlgraphBaseTest


logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)


def _props(target, updates):
    return models.Test().property_template(updates)


class TestPsqlGraphDriver(PsqlgraphBaseTest):

    def setUp(self):
        self.nid = str(uuid.uuid4())
        self._clear_tables()
        self.node = models.Test(self.nid)
        with self.g.session_scope() as session:
            session.add(self.node)

    def test_default_application_name(self):
        cmd = "select application_name from pg_stat_activity;"
        with self.g.session_scope() as s:
            s.merge(models.Test('a'))
            app_names = {r[0] for r in self.g.engine.execute(cmd)}
        self.assertIn(socket.gethostname(), app_names)

    def test_custom_application_name(self):
        cmd = "select application_name from pg_stat_activity;"
        custom_name = '_CUSTOM_NAME'
        
        g_ = PsqlGraphDriver(application_name=custom_name, **self.pg_conf)
        with g_.session_scope() as s:
            s.merge(models.Test('a'))
            app_names = {r[0] for r in self.g.engine.execute(cmd)}
        self.assertIn(custom_name, app_names)

    def test_property_set(self):
        new = {'key1': 'first property'}
        with self.g.session_scope() as session:
            self.node['key1'] = new['key1']
            session.merge(self.node)
        with self.g.session_scope() as session:
            expected = _props(models.Test, new)
            self.assertEqual(
                self.g.nodes().ids(self.nid).one().properties, expected)

    def test_property_update(self):
        new = {'key1': 'first property'}
        with self.g.session_scope() as session:
            self.node.properties.update(new)
            session.merge(self.node)
            session.commit()
        with self.g.session_scope() as session:
            expected = _props(models.Test, new)
            self.assertEqual(
                self.g.nodes().ids(self.nid).one().properties, expected)

    def test_property_merge(self):
        node = models.Test('a')
        node.key1 = 'first'
        node.key2 = 'first'
        with self.g.session_scope() as session:
            session.merge(node)
        node = models.Test('a')
        node.key1 = 'second'
        node.key1 = 'third'
        with self.g.session_scope() as session:
            session.merge(node)
        with self.g.session_scope() as session:
            node = self.g.nodes(models.Test).ids('a').one()
        self.assertEqual(node.key1, 'third')
        self.assertEqual(node.key2, 'first')

    def test_attribute_set(self):
        new = {'key1': 'first property'}
        with self.g.session_scope() as session:
            self.node.properties.update(new)
            self.node.key1 = 'first property'
            session.merge(self.node)
        with self.g.session_scope() as session:
            expected = _props(models.Test, new)
            self.assertEqual(
                self.g.nodes().ids(self.nid).one().properties, expected)

    def test_set_properties(self):
        new = {'key1': 'first property'}
        self.node.properties = new
        with self.g.session_scope() as session:
            session.merge(self.node)
        with self.g.session_scope() as session:
            expected = _props(models.Test, new)
            self.assertEqual(
                self.g.nodes().ids(self.nid).one().properties, expected)

    def test_validate_property(self):
        node = models.Test(str(uuid.uuid4()))
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
        with self.g.session_scope() as session:
            queried = self.g.nodes().ids(self.nid).one()
            queried.properties = new
            session.merge(queried)
        with self.g.session_scope() as session:
            expected = _props(models.Test, new)
            self.assertEqual(
                self.g.nodes().ids(self.nid).one().properties, expected)

    def test_update_query_result_properties(self):
        new = {'key1': 'first property', 'key2': 'first pass'}
        self.node.key1 = new['key2']
        with self.g.session_scope() as session:
            session.merge(self.node)
        with self.g.session_scope() as session:
            queried = self.g.nodes().ids(self.nid).one()
            queried.properties.update(new)
            session.merge(queried)
        with self.g.session_scope() as session:
            expected = _props(models.Test, new)
            self.assertEqual(
                self.g.nodes().ids(self.nid).one().properties, expected)

    def test_set_query_result_attribute(self):
        new = {'key1': 'first property', 'key2': 'first pass'}
        node = models.Test(self.nid)
        node.key2 = new['key2']
        with self.g.session_scope() as session:
            session.merge(node)
        with self.g.session_scope() as session:
            queried = self.g.nodes().ids(self.nid).one()
            queried.key1 = new['key1']
            session.merge(queried)
        with self.g.session_scope() as session:
            expected = _props(models.Test, new)
            self.assertEqual(
                self.g.nodes().ids(self.nid).one().properties, expected)

    def test_add_edge(self):
        src_id, dst_id = str(uuid.uuid4()), str(uuid.uuid4())
        src, dst = models.Test(src_id), models.Test(dst_id)
        edge = models.Edge1(src_id, dst_id)
        with self.g.session_scope() as session:
            session.add_all([src, dst])
            session.flush()
            session.add(edge)
            self.g.edges().filter(models.Edge1.src_id == src_id).one()

    def test_edge_attributes(self):
        foo = models.Foo('foonode')
        edge = models.Edge2(self.nid, 'foonode')
        with self.g.session_scope() as s:
            s.add_all((edge, foo))
        with self.g.session_scope() as s:
            n = self.g.nodes().ids(self.nid).one()
            self.assertEqual(n.foos[0].node_id, 'foonode')

    def test_type_enum(self):
        with self.assertRaises(ValidationError):
            models.Foo().fobble = 'test'

    def test_validate_enum(self):
        n = models.Foo('foonode')
        n.baz = 'allowed_1'
        n.baz = 'allowed_2'
        with self.assertRaises(ValidationError):
            n.baz = 'not allowed'

    def test_association_proxy(self):
        a = models.Test('a')
        b = models.Foo('b')
        c = models.Test('c')
        a.foos.append(b)
        a.tests = [c]
        with self.g.session_scope() as s:
            a, b, c = list(map(s.merge, (a, b, c)))
        with self.g.session_scope() as s:
            a = self.g.nodes(models.Test).ids('a').one()
            self.assertTrue(b in a.foos)
            self.assertEqual(a.tests, [c])

    def test_relationship_population_setter(self):
        a = models.Test('a')
        b = models.Foo('b')
        e = models.Edge2()
        e.src = a
        e.dst = b
        with self.g.session_scope() as s:
            a, b = list(map(s.merge, (a, b)))
            e2 = s.query(models.Edge2).src('a').dst('b').one()
            e = a._Edge2_out[0]
            self.assertEqual(e2, e)

    def test_nonnull(self):
        a = models.FooBar('a')
        a.bar = None
        a.props['bar'] = None
        a.properties = {'bar': None}
        with self.assertRaises(AssertionError):
            with self.g.session_scope() as s:
                s.merge(a)
        a.bar = True
        with self.g.session_scope() as s:
            s.merge(a)

    def test_relationship_population_constructer(self):
        a = models.Test('a')
        b = models.Foo('b')
        e = models.Edge2(src=a, dst=b)
        with self.g.session_scope() as s:
            a, b = list(map(s.merge, (a, b)))
            e2 = s.query(models.Edge2).src('a').dst('b').one()
            e = a._Edge2_out[0]
            self.assertEqual(e2, e)

    def test_cascade_delete(self):
        a = models.Test('a')
        b = models.Foo('b')
        a.foos = [b]
        with self.g.session_scope() as s:
            a, b = list(map(s.merge, (a, b)))
        with self.g.session_scope() as s:
            s.delete(self.g.nodes(models.Test).ids('a').one())
        with self.g.session_scope() as s:
            self.assertIsNone(self.g.nodes(models.Test).ids('a').scalar())
            self.assertIsNone(self.g.edges(models.Edge2).src('a').dst('b').scalar())

    def test_set_sysan(self):
        a = models.Test('a')
        with self.g.session_scope() as s:
            a = s.merge(a)
        with self.g.session_scope() as s:
            a = self.g.nodes(models.Test).ids('a').one()
            a = s.merge(a)
            a.sysan['key1'] = True
            s.merge(a)
        with self.g.session_scope() as s:
            n = self.g.nodes(models.Test).ids('a').one()
            print(n.sysan)
            self.assertTrue(n.sysan['key1'])

    def test_unchanged_properties_snapshot(self):
        nid = str(uuid.uuid4())
        a = models.Test(nid)
        value = '++ VALUE ++'
        with self.g.session_scope() as s:
            a.key1 = value
            s.merge(a)
            s.commit()
            a = self.g.nodes(models.Test).ids(nid).one()
            a.key1 = value
            s.merge(a)
            s.commit()
            self.assertEqual(a._history.all(), [])

    def test_unchanged_sysan_snapshot(self):
        nid = str(uuid.uuid4())
        a = models.Test(nid)
        value = '++ VALUE ++'
        with self.g.session_scope() as s:
            a.sysan['key1'] = value
            a = s.merge(a)
            s.commit()
            a.sysan['key1'] = value
            a = s.merge(a)
            s.commit()
            self.assertEqual(a._history.all(), [])

    def test_isolation_level(self):
        nid = str(uuid.uuid4())
        with self.g.session_scope() as s:
            s.merge(models.Test(nid))
        with self.assertRaises(sa.exc.OperationalError):
            with self.g.session_scope() as outer:
                a = outer.query(models.Test).filter(models.Test.node_id == nid).one()
                with self.g.session_scope(can_inherit=False) as inner:
                    b = inner.query(models.Test).filter(models.Test.node_id == nid).one()
                    b.sysan['inner'] = True
                    inner.merge(b)
                    inner.commit()
                a.sysan['outer'] = True

    def test_snapshot_on_delete(self):
        a = models.Test(str(uuid.uuid4()), key2=1)
        self.g.node_insert(a)
        with self.g.session_scope() as s:
            s.delete(a)
            self.assertEqual(a._history.one().properties['key2'], 1)

    def test_snapshot_sysan(self):
        a = models.Test(str(uuid.uuid4()), key2=1)
        with self.g.session_scope() as s:
            a = s.merge(a)
            s.commit()
            a.sysan['key'] = 1
            a = s.merge(a)
            s.commit()
            self.assertEqual(a._history.one().properties['key2'], 1)
            a.sysan['key'] = 3
            a = s.merge(a)

    def test_session_closing(self):
        with self.g.session_scope():
            nodes = self.g.nodes()
        with self.assertRaises(SessionClosedError):
            nodes.first()

    def test_sysan_sanitization(self):
        """It shouldn't be possible to set system annotations keys to
        anything but primitive values.

        """
        a = models.Test(str(uuid.uuid4()))
        with self.g.session_scope() as s:
            a = s.merge(a)
            with self.assertRaises(ValueError):
                a.sysan["foo"] = {"bar": "baz"}

    def test_session_timestamp(self):
        with self.g.session_scope() as s:
            self.assertIsNone(s._flush_timestamp)
            s.merge(models.Test(""))
            s.flush()
            self.assertIsNotNone(s._flush_timestamp)
        self.g.set_flush_timestamps = False
        with self.g.session_scope() as s:
            s.merge(models.Test(""))
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

        models.Test._session_hooks_before_insert = [
            add_key1,
            add_key2,
        ]

        try:
            with self.g.session_scope() as s:
                test = models.Test(node_id)
                s.merge(test)

            with self.g.session_scope():
                test = self.g.nodes(models.Test).ids(node_id).one()
                self.assertEqual(test.key1, 'value1')
                self.assertEqual(test.key2, 'value2')

        finally:
            models.Test._session_hooks_before_insert = []

    def test_custom_hooks_are_class_local(self):
        """Test that all custom hooks affect single classes."""

        self._clear_tables()
        node_id = 'test_locality'

        def bad_hook(target, session, *args, **kwargs):
            raise RuntimeError

        models.Test._session_hooks_before_insert = [
            bad_hook,
        ]

        try:
            with self.assertRaises(RuntimeError):
                with self.g.session_scope() as s:
                    test = models.Test(node_id)
                    s.merge(test)

            with self.g.session_scope() as s:
                foo = models.Foo(node_id)
                s.merge(foo)

            with self.g.session_scope():
                foo = self.g.nodes(models.Foo).ids(node_id).one()
                self.assertEqual(foo.bar, None)
                self.assertEqual(foo.baz, None)

        finally:
            models.Test._session_hooks_before_insert = []

    def test_custom_update_hooks(self):
        """Test that all custom insert hooks are called."""

        self._clear_tables()
        node_id = 'test_update'

        def add_key1(target, session, *args, **kwargs):
            target.key1 = 'value1'

        def add_key2(target, session, *args, **kwargs):
            target.key2 = 'value2'

        models.Test._session_hooks_before_update = [
            add_key1,
            add_key2,
        ]

        try:
            with self.g.session_scope() as s:
                assert not self.g.nodes(models.Test).ids(node_id).first()
                test = models.Test(node_id)
                s.merge(test)

            with self.g.session_scope():
                test = self.g.nodes(models.Test).ids(node_id).one()
                self.assertEqual(test.key1, None)
                self.assertEqual(test.key2, None)

            with self.g.session_scope() as s:
                test = models.Test(node_id)
                s.merge(test)

            with self.g.session_scope():
                test_ = self.g.nodes(models.Test).ids(node_id).one()
                self.assertEqual(test_.key1, 'value1')
                self.assertEqual(test_.key2, 'value2')

        finally:
            models.Test._session_hooks_before_update = []

    def test_custom_delete_hooks(self):
        """Test that all custom pre-delete hooks are called."""

        self._clear_tables()
        node_id = 'test_update'
        new_node_id1 = 'pre-delete-1'
        new_node_id2 = 'pre-delete-2'

        def add_new_node1(target, session, *args, **kwargs):
            session.merge(models.Test(new_node_id1))

        def add_new_node2(target, session, *args, **kwargs):
            session.merge(models.Test(new_node_id2))

        models.Test._session_hooks_before_delete = [
            add_new_node1,
            add_new_node2,
        ]

        try:
            with self.g.session_scope() as s:
                assert not self.g.nodes(models.Test).ids(node_id).first()
                test = models.Test(node_id)
                test = s.merge(test)
                s.commit()
                s.delete(test)

            with self.g.session_scope():
                self.g.nodes(models.Test).ids(new_node_id1).one()
                self.g.nodes(models.Test).ids(new_node_id2).one()
                self.assertFalse(self.g.nodes(models.Test).ids(node_id).scalar())

        finally:
            models.Test._session_hooks_before_delete = []

    def test_null_props_unset(self):
        self._clear_tables()

        with self.g.session_scope() as session:
            session.merge(models.Test('a'))
            session.merge(models.Test('b', key1='1', key2='2', key3='3'))

        with self.g.session_scope():
            self.g.nodes(models.Test).null_props('key1').one()
            self.g.nodes(models.Test).null_props(['key1', 'key2']).one()
            self.g.nodes(models.Test).null_props('key1', 'key2').one()
            self.g.nodes(models.Test).null_props(['key1', 'key2'], 'key3').one()
            self.g.nodes(models.Test).null_props('key1').one()

    def test_null_props_set(self):
        self._clear_tables()

        with self.g.session_scope() as session:
            session.merge(models.Test('a', key1=None, key2=None, key3=None))
            session.merge(models.Test('b', key1='1', key2='2', key3='3'))

        with self.g.session_scope():
            self.g.nodes(models.Test).null_props('key1').one()
            self.g.nodes(models.Test).null_props(['key1', 'key2']).one()
            self.g.nodes(models.Test).null_props('key1', 'key2').one()
            self.g.nodes(models.Test).null_props(['key1', 'key2'], 'key3').one()
            self.g.nodes(models.Test).null_props('key1').one()
