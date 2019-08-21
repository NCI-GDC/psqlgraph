from psqlgraph import Node, Edge, pg_property


class FakeDictionary(object):
    def __init__(self):
        self.schema = {
            'test': {
                'required': ['key1'],
                'properties': {
                    'key1': {'type': 'string'},
                    'key2': {'type': 'string'},
                    'key3': {'type': 'string'},
                    'new_key': {'type': 'string'},
                    'timestamp': {
                        'oneOf': [
                            {'type': 'string', 'format': 'date-time'},
                            {'type': 'integer'},
                        ]
                    }
                },
                'links': [
                    {'name': 'tests'},
                    {'name': 'foos'},
                ],
            },
            'foo': {
                'properties': {
                    'bar': {'type': 'string'},
                    'baz': {'enum': ['allowed_1', 'allowed_2']},
                    'fobble': {
                        'type': 'integer',
                        'minimum': 20,
                        'maximum': 30,
                    }
                },
                'links': [
                    {'name': 'foobars'},
                ],
            },
            'foo_bar': {
                'properties': {
                    'bar': {'type': 'string'}
                },
                'links': [],
            }
        }


class Edge1(Edge):

    __src_class__ = 'Test'
    __dst_class__ = 'Test'
    __src_dst_assoc__ = 'tests'
    __dst_src_assoc__ = 'sub_tests'

    @pg_property(str, int)
    def test(self, value):
        self._set_property('test', value)

    @pg_property
    def key1(self, value):
        self._set_property('key1', value)

    @pg_property
    def key2(self, value):
        self._set_property('key2', value)


class Edge2(Edge):

    __label__ = 'test_edge_2'
    __src_class__ = 'Test'
    __dst_class__ = 'Foo'
    __src_dst_assoc__ = 'foos'
    __dst_src_assoc__ = 'tests'


class Edge3(Edge):

    __src_class__ = 'Foo'
    __dst_class__ = 'FooBar'
    __src_dst_assoc__ = 'foobars'
    __dst_src_assoc__ = 'foos'


class TestToFooBarEdge(Edge):

    __src_class__ = 'Test'
    __dst_class__ = 'FooBar'
    __src_dst_assoc__ = 'foobars'
    __dst_src_assoc__ = 'tests'


class Test(Node):

    _pg_edges = {}

    @pg_property
    def key1(self, value):
        assert isinstance(value, (str, type(None)))
        assert value != 'bad_value'
        self._set_property('key1', value)

    @pg_property
    def key2(self, value):
        self._set_property('key2', value)

    @pg_property
    def key3(self, value):
        self._set_property('key3', value)

    @pg_property
    def new_key(self, value):
        self._set_property('new_key', value)

    @pg_property(int, str)
    def timestamp(self, value):
        self._set_property('timestamp', value)


class Foo(Node):

    __label__ = 'foo'

    _pg_edges = {}

    @pg_property
    def bar(self, value):
        self._set_property('bar', value)

    @pg_property(enum=('allowed_1', 'allowed_2'))
    def baz(self, value):
        self._set_property('baz', value)

    @pg_property(int)
    def fobble(self, value):
        self._set_property('fobble', value)


class FooBar(Node):

    __label__ = 'foo_bar'
    __nonnull_properties__ = ['bar']

    _pg_edges = {}

    @pg_property
    def bar(self, value):
        self._set_property('bar', value)


Test._pg_edges.update({
    'tests': {
        'backref': '_tests',
        'type': Test,
    },
    'foos': {
        'backref': 'tests',
        'type': Foo,
    }
})

Foo._pg_edges.update({
    'foobars': {
        'backref': 'foos',
        'type': FooBar,
    },
    'tests': {
        'backref': 'foos',
        'type': Test,
    }
})

FooBar._pg_edges.update({
    'foos': {
        'backref': 'foobars',
        'type': Foo,
    }
})
