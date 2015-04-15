from datetime import datetime
from sqlalchemy import Column, Text, DateTime, UniqueConstraint, \
    event, ForeignKey
from sqlalchemy.ext.declarative import AbstractConcreteBase, declared_attr
from sqlalchemy.orm import object_session, sessionmaker

from base import ORMBase
from voided_edge import VoidedEdge


class Edge(AbstractConcreteBase, ORMBase):

    _label = Column(
        Text,
        nullable=False,
        primary_key=True,
    )

    @classmethod
    def _node_id_column(self, table):
        if not table:
            return None
        return Column(
            Text,
            ForeignKey(
                '{}.node_id'.format(table),
                ondelete="CASCADE"),
            primary_key=True,
            nullable=False
        )

    @declared_attr
    def src_id(self):
        return self._node_id_column(self.__src_label__)

    @declared_attr
    def dst_id(self):
        return self._node_id_column(self.__dst_label__)

    @declared_attr
    def __table_args__(cls):
        return tuple()

    def __init__(self, src_id=None, dst_id=None, properties={}, acl=[],
                 system_annotations={}, label=None):
        self._props = {}
        self.system_annotations = system_annotations
        self.acl = acl
        self.label = label or self.__mapper_args__['polymorphic_identity']
        self.properties = properties
        self.src_id = src_id
        self.dst_id = dst_id

    def __repr__(self):
        return '<{}(({})-[{}]->({})>'.format(
            self.__class__.__name__, self.src_id, self.label, self.dst_id)

    @classmethod
    def get_subclass(cls, label):
        for c in cls.__subclasses__():
            clabel = getattr(c, '__mapper_args__', {}).get(
                'polymorphic_identity', None)
            if clabel == label:
                return c
        raise KeyError('Node has no subclass {}'.format(label))

    @classmethod
    def get_subclass_table_names(label):
        return [s.__tablename__ for s in Edge.__subclasses__()]

    @classmethod
    def get_subclasses(cls, label):
        return [s for s in cls.__subclasses__()]

    def snapshot_existing(self, session, existing):
        if existing:
            voided_node = VoidedEdge(existing)
            session.add(voided_node)

    def merge_onto_existing(self, session, existing):
        if not existing:
            self._props = self.properties
        else:
            temp = self.property_template()
            temp.update(existing._props)
            temp.update(self._props)
            self._props = temp

    def lookup_existing(self, session):
        Clean = sessionmaker()
        Clean.configure(bind=session.bind)
        clean = Clean()
        return clean.query(Edge).filter(Edge.src_id == self.src_id)\
                                .filter(Edge.dst_id == self.dst_id)\
                                .filter(Edge._label == self.label)\
                                .scalar()


class _PseudoEdge(Edge):
    """Necessary to have atleast one class inherit from abstract Edge"""
    __src_label__ = None
    __dst_label__ = None


@event.listens_for(Edge, 'before_insert', propagate=True)
def receive_before_insert(mapper, connection, edge):
    edge._props = edge.properties
