from psqlgraph import Edge, Node, pg_property


class FakeDictionary:
    def __init__(self):
        self.schema = {
            "test": {
                "required": ["key1"],
                "properties": {
                    "key1": {"type": "string"},
                    "key2": {"type": "string"},
                    "key3": {"type": "string"},
                    "new_key": {"type": "string"},
                    "timestamp": {
                        "oneOf": [
                            {"type": "string", "format": "date-time"},
                            {"type": "integer"},
                        ]
                    },
                },
                "links": [
                    {"name": "tests"},
                    {"name": "foos"},
                ],
            },
            "foo": {
                "properties": {
                    "bar": {"type": "string"},
                    "baz": {"enum": ["allowed_1", "allowed_2"]},
                    "fobble": {
                        "type": "integer",
                        "minimum": 20,
                        "maximum": 30,
                    },
                    "studies": {
                        "type": "array",
                        "items": {
                            "enum": ["N/A", "Unknown"],
                        },
                    },
                    "ages": {"type": "array", "items": {"type": "integer"}},
                },
                "links": [{"name": "foobars"}],
            },
            "foo_bar": {
                "properties": {
                    "bar": {"type": "string"},
                },
                "links": [],
            },
            "test_default_value": {
                "properties": {
                    "property_with_default": {
                        "default": "open",
                        "enum": ["open", "submitted", "closed", "legacy"],
                    },
                    "property_without_default": {"type": "string"},
                },
                "links": [],
            },
            "circle_1": {"properties": {}, "links": [{"name": "edge_4"}]},
            "circle_2": {"properties": {}, "links": [{"name": "edge_5"}]},
        }


class Edge1(Edge):

    __src_class__ = "Test"
    __dst_class__ = "Test"
    __src_dst_assoc__ = "tests"
    __dst_src_assoc__ = "sub_tests"

    @pg_property(str, int)
    def test(self, value):
        self._set_property("test", value)

    @pg_property
    def key1(self, value):
        self._set_property("key1", value)

    @pg_property
    def key2(self, value):
        self._set_property("key2", value)


class Edge2(Edge):

    __label__ = "test_edge_2"
    __src_class__ = "Test"
    __dst_class__ = "Foo"
    __src_dst_assoc__ = "foos"
    __dst_src_assoc__ = "tests"


class Edge3(Edge):

    __src_class__ = "Foo"
    __dst_class__ = "FooBar"
    __src_dst_assoc__ = "foobars"
    __dst_src_assoc__ = "foos"


# edge4 and edge5 are used to test special case, Foo->Bar and Bar->Foo are both valid
# edges.
class Edge4(Edge):

    __label__ = "edge4"

    __src_class__ = "Circle1"
    __dst_class__ = "Circle2"
    __src_dst_assoc__ = "circle_2a"
    __dst_src_assoc__ = "circle_1a"


class Edge5(Edge):

    __label__ = "edge5"

    __src_class__ = "Circle2"
    __dst_class__ = "Circle1"
    __src_dst_assoc__ = "circle_1b"
    __dst_src_assoc__ = "circle_2b"


class TestToFooBarEdge(Edge):

    __src_class__ = "Test"
    __dst_class__ = "FooBar"
    __src_dst_assoc__ = "foobars"
    __dst_src_assoc__ = "tests"


class Test(Node):

    _pg_edges = {}

    @pg_property
    def key1(self, value):
        assert isinstance(value, (str, type(None)))
        assert value != "bad_value"
        self._set_property("key1", value)

    @pg_property
    def key2(self, value):
        self._set_property("key2", value)

    @pg_property
    def key3(self, value):
        self._set_property("key3", value)

    @pg_property
    def new_key(self, value):
        self._set_property("new_key", value)

    @pg_property(int, str)
    def timestamp(self, value):
        self._set_property("timestamp", value)


class Foo(Node):

    __label__ = "foo"

    _pg_edges = {}

    @pg_property
    def bar(self, value):
        self._set_property("bar", value)

    @pg_property(enum=("allowed_1", "allowed_2"))
    def baz(self, value):
        self._set_property("baz", value)

    @pg_property(int)
    def fobble(self, value):
        self._set_property("fobble", value)

    @pg_property(list)
    def studies(self, value):
        self._set_property("studies", value)

    @pg_property(list)
    def ages(self, value):
        self._set_property("ages", value)


class Circle1(Node):

    __label__ = "circle_1"

    _pg_edges = {}


class Circle2(Node):

    __label__ = "circle_2"

    _pg_edges = {}


class FooBar(Node):

    __label__ = "foo_bar"
    __nonnull_properties__ = ["bar"]

    _pg_edges = {}

    @pg_property
    def bar(self, value):
        self._set_property("bar", value)


class TestDefaultValue(Node):
    __label__ = "test_default_value"

    _pg_edges = {}

    _defaults = {"property_with_default": "open"}

    @pg_property(enum=("open", "submitted", "closed", "legacy"))
    def property_with_default(self, value):
        self._set_property("property_with_default", value)

    @pg_property
    def property_without_default(self, value):
        self._set_property("property_without_default", value)


Test._pg_edges.update(
    {
        "tests": {
            "backref": "_tests",
            "type": Test,
        },
        "foos": {
            "backref": "tests",
            "type": Foo,
        },
    }
)


Foo._pg_edges.update(
    {
        "foobars": {
            "backref": "foos",
            "type": FooBar,
        },
        "tests": {
            "backref": "foos",
            "type": Test,
        },
    }
)

Circle1._pg_edges.update(
    {
        "circle_2a": {
            "backref": "bars",
            "type": Circle2,
        },
        "circle_2b": {
            "backref": "bars",
            "type": Circle2,
        },
    }
)

Circle2._pg_edges.update(
    {
        "circle_1a": {
            "backref": "bars",
            "type": Circle1,
        },
        "circle_1b": {
            "backref": "bars",
            "type": Circle1,
        },
    }
)


FooBar._pg_edges.update(
    {
        "foos": {
            "backref": "foobars",
            "type": Foo,
        }
    }
)
