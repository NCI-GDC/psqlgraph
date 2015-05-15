from base import ORMBase
from edge import Edge
from sqlalchemy import Column, Text, UniqueConstraint, event
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import AbstractConcreteBase, declared_attr
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from voided_node import VoidedNode


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
                if not hasattr(cls, name_in):
                    edge_in = relationship(
                        name,
                        foreign_keys=[scls.dst_id],
                        backref='dst',
                        cascade='all, delete, delete-orphan',
                    )
                    setattr(cls, name_in, edge_in)
                    cls._edges_in.append(name_in)
                    dst_ids.append(scls.dst_id)
                cls._set_association_proxy(scls, dst_assoc, name_in, 'src')
            if scls.__src_class__ == cls.__name__:
                if not hasattr(cls, name_out):
                    edge_out = relationship(
                        name,
                        foreign_keys=[scls.src_id],
                        backref='src',
                        cascade='all, delete, delete-orphan',
                    )
                    setattr(cls, name_out, edge_out)
                    cls._edges_out.append(name_out)
                    src_ids.append(scls.src_id)
                cls._set_association_proxy(scls, src_assoc, name_out, 'dst')

    @classmethod
    def _set_association_proxy(cls, edge_cls, attr_name, edge_name, direction):
        rel = association_proxy(
            edge_name,
            direction,
            creator=lambda node: edge_cls(**{direction: node})
        )
        setattr(cls, attr_name, rel)

    def get_edges(self):
        for edge_in in self.edges_in:
            yield edge_in
        for edge_out in self.edges_out:
            yield edge_out

    def __init__(self, node_id=None, properties={}, acl=[],
                 system_annotations={}, label=None, **kwargs):
        self._props = {}
        self.system_annotations = system_annotations
        self.acl = acl
        self.label = label or self.get_label()
        self.properties = properties
        self.properties.update(kwargs)
        self.node_id = node_id

    def __repr__(self):
        return '<{}({node_id})>'.format(
            self.__class__.__name__,
            node_id=self.node_id)

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__)
            and self.node_id == other.node_id
        )

    def __ne__(self, other):
        return not self.__eq__(other)

    def copy(self):
        node = Node(
            node_id=self.node_id,
            acl=self.acl,
            _system_annotations=self.system_annotations,
            label=self.label,
        )
        return node

    @classmethod
    def get_subclass(cls, label):
        for c in cls.__subclasses__():
            if c.get_label() == label:
                return c
        return None

    @classmethod
    def get_subclass_named(cls, name):
        for c in cls.__subclasses__():
            if c.__name__ == name:
                return c
        raise KeyError('Node has no subclass named {}'.format(name))

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

    def _snapshot_existing(self, session, old_props, old_sysan):
        temp = self.__class__(self.node_id, old_props, self.acl,
                              old_sysan, self.label)
        voided = VoidedNode(temp)
        session.add(voided)


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
