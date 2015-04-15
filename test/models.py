from sqlalchemy import Column, Text, BigInteger, Integer,\
    UniqueConstraint, ForeignKey, DateTime, Table
from sqlalchemy.orm import relationship
from psqlgraph import Node, Edge
from sqlalchemy.ext.hybrid import hybrid_property


class Test(Node):

    @hybrid_property
    def key1(self):
        return self.properties['key1']

    @key1.setter
    def key1(self, value):
        self.properties['key1'] = value

    @hybrid_property
    def key2(self):
        return self.properties['key2']

    @key2.setter
    def key2(self, value):
        self.properties['key2'] = value

    @hybrid_property
    def key3(self):
        return self.properties['key3']

    @key3.setter
    def key3(self, value):
        self.properties['key3'] = value

    @hybrid_property
    def new_key(self):
        return self.properties['new_key']

    @new_key.setter
    def new_key(self, value):
        self.properties['new_key'] = value

    @hybrid_property
    def timestamp(self):
        return self.properties['timestamp']

    @timestamp.setter
    def timestamp(self, value):
        self.properties['timestamp'] = value


class Foo(Node):

    @hybrid_property
    def bar(self):
        return self.properties['bar']

    @bar.setter
    def bar(self, value):
        self.properties['bar'] = value


class Edge1(Edge):
    __src_label__ = 'test'
    __dst_label__ = 'test'


class Edge2(Edge):
    __src_label__ = 'test'
    __dst_label__ = 'foo'
