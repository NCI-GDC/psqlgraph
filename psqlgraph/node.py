from datetime import datetime
from sqlalchemy import Column, Text, DateTime, UniqueConstraint, event
from sqlalchemy.dialects.postgres import ARRAY, JSONB
from sqlalchemy.ext.declarative import AbstractConcreteBase, declared_attr
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import object_session, sessionmaker
import copy

from base import ORMBase
from voided_node import VoidedNode
from util import sanitize


class Node(AbstractConcreteBase, ORMBase):

    node_id = Column(
        Text,
        primary_key=True,
        nullable=False,
    )

    @declared_attr
    def __table_args__(cls):
        name = cls.__name__.lower()
        return (UniqueConstraint(
            'node_id', name='_{}_id_uc'.format(name)),)

    def __init__(self, node_id=None, properties={}, acl=[],
                 system_annotations={}, label=None):
        self._props = {}
        self.system_annotations = system_annotations
        self.acl = acl
        self.label = label or self.__mapper_args__['polymorphic_identity']
        self.properties = properties
        self.node_id = node_id

    def __repr__(self):
        return '<{}({node_id}, {label})>'.format(
            self.__class__.__name__, node_id=self.node_id, label=self.label)

    def copy(self):
        node = Node(
            node_id=self.node_id,
            acl=self.acl,
            _system_annotations=self.system_annotations,
            label=self.label,
        )
        return node

    def merge(self, acl=None, system_annotations={}, properties={}):

        if system_annotations:
            self.system_annotations = copy.deepcopy(self.system_annotations)
            self.system_annotations.update(system_annotations)

        for key, value in properties.items():
            setattr(self, key, value)

        if acl is not None:
            self.acl = acl

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
        return [s.__tablename__ for s in Node.__subclasses__()]

    @classmethod
    def get_subclasses(cls, label):
        return [s for s in cls.__subclasses__()]

    @property
    def _history(self):
        session = self.get_session()
        if not session:
            raise RuntimeError(
                '{} not bound to a session. Try get_history(session).'.format(
                    self))
        return self.get_history(session)

    def get_history(self, session):
        assert self.label, 'Specify label for node history'
        return session.query(VoidedNode)\
                      .filter(VoidedNode.node_id == self.node_id)\
                      .filter(VoidedNode.label == self.label)\
                      .order_by(VoidedNode.voided.desc())

    def snapshot_existing(self, session, existing):
        if existing:
            voided_node = VoidedNode(existing)
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
        return clean.query(Node).filter(Node.node_id == self.node_id)\
                                .filter(Node._label == self.label)\
                                .scalar()


def PolyNode(node_id=None, label=None, acl=[], system_annotations={},
             properties={}):
    assert label, 'You cannot create a PolyNode without a label.'
    Type = Node.get_subclass(label)
    return Type(
        node_id=node_id,
        properties=properties,
        acl=acl,
        system_annotations=system_annotations,
        label=label
    )


@event.listens_for(Node, 'before_insert', propagate=True)
def receive_before_insert(mapper, connection, node):
    node._props = node.properties
