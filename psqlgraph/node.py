from datetime import datetime
from sqlalchemy import Column, Text, DateTime, UniqueConstraint, event
from sqlalchemy.dialects.postgres import ARRAY, JSONB
from sqlalchemy.ext.declarative import AbstractConcreteBase, declared_attr
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import object_session, sessionmaker, relationship
import copy

from base import ORMBase
from voided_node import VoidedNode
from util import sanitize
from edge import Edge

from sqlalchemy.ext.associationproxy import association_proxy


DST_SRC_ASSOC = '__dst_src_assoc__'
SRC_DST_ASSOC = '__src_dst_assoc__'


def reverse_lookup(dictionary, search_val):
    for key, val in dictionary.iteritems():
        if val == search_val:
            yield key


class Node(AbstractConcreteBase, ORMBase):

    @declared_attr
    def _edges_out(self):
        return list()

    @declared_attr
    def _edges_in(self):
        return list()

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

    @hybrid_property
    def edges_in(self):
        return [e for rel in self._edges_in for e in getattr(self, rel)]

    @hybrid_property
    def edges_out(self):
        return [e for rel in self._edges_out for e in getattr(self, rel)]

    @classmethod
    def __declare_last__(cls):
        src_ids, dst_ids = [], []
        for scls in Edge.get_subclasses():
            name = scls.__name__
            name_in = '_{}_in'.format(name)
            name_out = '_{}_out'.format(name)
            src_assoc = getattr(scls, SRC_DST_ASSOC)
            dst_assoc = getattr(scls, DST_SRC_ASSOC)
            if scls.__dst_class__ == cls.__name__:
                edge_in = relationship(
                    name, foreign_keys=[scls.dst_id], viewonly=True)
                setattr(cls, name_in, edge_in)
                cls._edges_in.append(name_in)
                dst_ids.append(scls.dst_id)
                cls._set_association_proxy(scls, dst_assoc, 'src')
            if scls.__src_class__ == cls.__name__:
                edge_out = relationship(
                    name, foreign_keys=[scls.src_id], viewonly=True)
                setattr(cls, name_out, edge_out)
                cls._edges_out.append(name_out)
                src_ids.append(scls.src_id)
                cls._set_association_proxy(scls, src_assoc, 'dst')

    @classmethod
    def _set_association_proxy(cls, edge_cls, attr_name, direction):
        if not attr_name:
            return
        src_cls = edge_cls.__src_class__
        dst_cls = edge_cls.__dst_class__
        edge_cls_name = edge_cls.__name__
        edge_table = edge_cls.__table__
        if hasattr(cls, attr_name):
            raise AttributeError((
                "Attribute '{}' already exists on {}, cannot add "
                "association proxy specified at {}.{} while attempting "
                "to create relationship [{}.{} => {}]."
            ).format(
                attr_name, cls, edge_cls_name,
                DST_SRC_ASSOC if direction == 'dst' else SRC_DST_ASSOC,
                cls.__name__, attr_name,
                dst_cls if direction == 'dst' else src_cls))
        if direction == 'src':
            rel = relationship(
                src_cls,
                secondary=edge_table,
                primaryjoin='({src}.node_id == {edge}.src_id)'.format(
                    src=src_cls, edge=edge_cls_name),
                foreign_keys=edge_cls.src_id,
            )
        else:
            rel = relationship(
                dst_cls,
                secondary=edge_table,
                primaryjoin='({dst}.node_id == {edge}.dst_id)'.format(
                    dst=dst_cls, edge=edge_cls_name),
                foreign_keys=edge_cls.dst_id,
            )
        # rel = association_proxy(edge_name, direction)
        setattr(cls, attr_name, rel)

    def get_edges(self):
        for edge_in in self.edges_in:
            yield edge_in
        for edge_out in self.edges_out:
            yield edge_out

    def __init__(self, node_id=None, properties={}, acl=[],
                 system_annotations={}, label=None):
        self._props = {}
        self.system_annotations = system_annotations
        self.acl = acl
        self.label = label or self.get_label()
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
        if system_annotations is not None:
            self._sysan.update(system_annotations)
        for key, value in properties.items():
            setattr(self, key, value)
        if acl is not None:
            self.acl = acl

    @classmethod
    def get_subclass(cls, label):
        for c in cls.__subclasses__():
            if c.get_label() == label:
                return c
        raise KeyError('Node has no subclass {}'.format(label))

    @classmethod
    def get_subclass_table_names(label):
        return [s.__tablename__ for s in Node.__subclasses__()]

    @classmethod
    def get_subclasses(cls):
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

    def _snapshot_existing(self, session, existing):
        if existing:
            voided_node = VoidedNode(existing)
            session.add(voided_node)

    def _merge_onto_existing(self, session, existing):
        if not existing:
            self._props = self.properties
        else:
            temp = self.property_template()
            temp.update(existing._props)
            temp.update(self._props)
            self._props = temp

    def _lookup_existing(self, session):
        clean = self._get_clean_session(session)
        res = clean.query(Node).filter(Node.node_id == self.node_id)\
                               .filter(Node._label == self.label)\
                               .scalar()
        clean.expunge_all()
        clean.close()
        return res

    def _check_unique(self):
        clean = self._get_clean_session()
        current = self.get_session()
        others = clean.query(Node).filter(Node.node_id == self.node_id)\
                                  .filter(Node._label != self.label)
        assert others.count() == 0,\
            'There is another node with id "{}" in the database'.format(
                self.node_id)
        others = current.query(Node).filter(Node.node_id == self.node_id)\
                                    .filter(Node._label != self.label)
        assert others.count() == 0,\
            'There is another node with id "{}" in current session'.format(
                self.node_id)
        clean.expunge_all()
        clean.close()


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
    # node._check_unique()
    node._validate()
    node._props = node.properties
