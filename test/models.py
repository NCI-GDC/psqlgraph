from sqlalchemy import Column, Text, BigInteger, Integer,\
    UniqueConstraint, ForeignKey, DateTime, Table
from sqlalchemy.orm import relationship, validates
from psqlgraph import Node, Edge
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.ext.declarative import AbstractConcreteBase, declared_attr
from psqlgraph.base import CommonBase


class Edge1(Edge):

    __src_label__ = 'test'
    __dst_label__ = 'test'

    @hybrid_property
    def test(self):
        return self._get_property('test')

    @test.setter
    def test(self, value):
        self._set_property('test', value)

    @hybrid_property
    def key1(self):
        return self._get_property('key1')

    @key1.setter
    def key1(self, value):
        self._set_property('key1', value)

    @hybrid_property
    def key2(self):
        return self._get_property('key2')

    @key2.setter
    def key2(self, value):
        self._set_property('key2', value)


class Edge2(Edge):
    __label__ = 'test_edge_2'

    __src_label__ = 'test'
    __dst_label__ = 'foo'


class Test(Node):

    edges_out = relationship("Edge1", foreign_keys=[Edge1.src_id])
    edges_in = relationship("Edge1", foreign_keys=[Edge1.dst_id])

    @hybrid_property
    def key1(self):
        return self._get_property('key1')

    @key1.setter
    def key1(self, value):
        assert isinstance(value, (str, type(None)))
        self._set_property('key1', value)

    @validates('key1')
    def validate_key1(self, key, value):
        print 'validating', key, value
        assert value != 'bad_value'
        return value

    @hybrid_property
    def key2(self):
        return self._get_property('key2')

    @key2.setter
    def key2(self, value):
        self._set_property('key2', value)

    @hybrid_property
    def key3(self):
        return self._get_property('key3')

    @key3.setter
    def key3(self, value):
        self._set_property('key3', value)

    @hybrid_property
    def new_key(self):
        return self._get_property('new_key')

    @new_key.setter
    def new_key(self, value):
        self._set_property('new_key', value)

    @hybrid_property
    def timestamp(self):
        return self._get_property('timestamp')

    @timestamp.setter
    def timestamp(self, value):
        self._set_property('timestamp', value)


class Foo(Node):

    __label__ = 'foo'

    @hybrid_property
    def bar(self):
        return self._get_property('bar')

    @bar.setter
    def bar(self, value):
        self._set_property('bar', value)


class FooBar(Node):

    __label__ = 'foo_bar'
    __nonnull_properties__ = ['bar']

    @hybrid_property
    def bar(self):
        return self._get_property('bar')

    @bar.setter
    def bar(self, value):
        self._set_property('bar', value)
