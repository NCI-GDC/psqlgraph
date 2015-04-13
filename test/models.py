from sqlalchemy import Column, Text, BigInteger, Integer,\
    UniqueConstraint, ForeignKey, DateTime, Table
from sqlalchemy.orm import relationship
from psqlgraph.node import Node, Base, Edge


class TestNode(Node):

    __tablename__ = 'node_test'
    __table_args__ = (UniqueConstraint('_id', name='_test_id_uc'),)
    __mapper_args__ = {'polymorphic_identity': 'test'}
    _id = Column(Text, ForeignKey('_nodes._id'), primary_key=True)

    key1 = Column(Text)
    key2 = Column(Integer)
    key3 = Column(DateTime)
    new_key = Column(Text)
    timestamp = Column(DateTime)

    def __init__(self, *args, **kwargs):
        super(TestNode, self).__init__(*args, **kwargs)


class FooEdge(Edge, Base):
    __tablename__ = 'edge_foo'
    src_id = Column(Text, ForeignKey('node_foo._id'), primary_key=True)
    dst_id = Column(Text, ForeignKey('node_foo._id'), primary_key=True)

    def __init__(self, *args, **kwargs):
        pass


class FooNode(Node):

    __tablename__ = 'node_foo'
    __table_args__ = (UniqueConstraint('_id', name='_foo_id_uc'),)
    __mapper_args__ = {'polymorphic_identity': 'foo'}
    _id = Column(Text, ForeignKey('_nodes._id'), primary_key=True)

    bar = Column(BigInteger)

    def __init__(self, *args, **kwargs):
        super(FooNode, self).__init__(*args, **kwargs)
