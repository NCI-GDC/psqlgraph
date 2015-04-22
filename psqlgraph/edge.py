from datetime import datetime
from sqlalchemy import Column, Text, DateTime, UniqueConstraint, \
    event, ForeignKey
from sqlalchemy.ext.declarative import AbstractConcreteBase, declared_attr
from sqlalchemy.orm import object_session, sessionmaker, relationship
from sqlalchemy.ext.hybrid import hybrid_property

from base import ORMBase
from voided_edge import VoidedEdge


def IDColumn(tablename):
    return Column(
        Text, ForeignKey(
            '{}.node_id'.format(tablename),
            ondelete="CASCADE",
            deferrable=True,
            initially="DEFERRED",
        ), primary_key=True, nullable=False)


def edge_attributes(name, src_class, dst_class,
                    src_table=None, dst_table=None):
    src_table = src_table or src_class.lower()
    dst_table = dst_table or dst_class.lower()
    src_id = IDColumn(src_table)
    dst_id = IDColumn(dst_table)
    src = relationship(src_class, foreign_keys=[src_id])
    dst = relationship(dst_class, foreign_keys=[dst_id])
    return (src_id, dst_id, src, dst)


class Edge(AbstractConcreteBase, ORMBase):

    _label = Column(
        Text,
        nullable=False,
        primary_key=True,
    )

    __src_table__ = None
    __dst_table__ = None

    src_id, dst_id, src, dst = None, None, None, None

    @declared_attr
    def src_id(cls):
        if cls.__name__ == 'Edge':
            return
        src_table = cls.__src_table__ or cls.__src_class__.lower()
        src_id = IDColumn(src_table)
        return src_id

    @declared_attr
    def dst_id(cls):
        if cls.__name__ == 'Edge':
            return
        dst_table = cls.__dst_table__ or cls.__dst_class__.lower()
        dst_id = IDColumn(dst_table)
        return dst_id

    @classmethod
    def __declare_last__(cls):
        if cls == Edge:
            return
        assert hasattr(cls, '__src_class__'),\
            'You must declare __src_class__ for {}'.format(cls)
        assert hasattr(cls, '__dst_class__'),\
            'You must declare __dst_class__ for {}'.format(cls)
        assert hasattr(cls, '__src_dst_assoc__'),\
            'You must declare __src_dst_assoc__ for {}'.format(cls)
        assert hasattr(cls, '__dst_src_assoc__'),\
            'You must declare __dst_src_assoc__ for {}'.format(cls)
        cls.src = relationship(cls.__src_class__, foreign_keys=[cls.src_id])
        cls.dst = relationship(cls.__dst_class__, foreign_keys=[cls.dst_id])

    @declared_attr
    def __table_args__(cls):
        return tuple()

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

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
        scls = cls._get_subclasses_labeled(label)
        if len(scls) > 1:
            raise KeyError(
                'More than one Edge with label {} found: {}'.format(
                    label, scls))
        if not scls:
            KeyError('Edge has no subclass {}'.format(label))
        return scls[0]

    @classmethod
    def _get_subclasses_labeled(cls, label):
        return [c for c in cls.__subclasses__()
                if c.get_label() == label]

    @classmethod
    def _get_edges_with_src(cls, src_class_name):
        return [c for c in cls.__subclasses__()
                if c.__src_class__ == src_class_name]

    @classmethod
    def _get_edges_with_dst(cls, dst_class_name):
        return [c for c in cls.__subclasses__()
                if c.__dst_class__ == dst_class_name]

    @classmethod
    def get_subclass_table_names(label):
        return [s.__tablename__ for s in Edge.__subclasses__()]

    @classmethod
    def get_subclasses(cls):
        return [s for s in cls.__subclasses__()]

    def _snapshot_existing(self, session, existing):
        if existing:
            voided_node = VoidedEdge(existing)
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
        res = clean.query(Edge).filter(Edge.src_id == self.src_id)\
                               .filter(Edge.dst_id == self.dst_id)\
                               .filter(Edge._label == self.label)\
                               .scalar()
        clean.expunge_all()
        clean.close()
        return res


def PolyEdge(src_id=None, dst_id=None, label=None, acl=[],
             system_annotations={}, properties={}):
    assert label, 'You cannot create a PolyEdge without a label.'
    try:
        Type = Edge.get_subclass(label)
    except Exception as e:
        raise RuntimeError(
            "{}: Unable to determine edge type. If there are more than one "
            "edges with label {}, you need to specify src_label and dst_label"
            "using the PsqlGraphDriver.get_PolyEdge())"
        ).format(e, label)

    return Type(
        src_id=src_id,
        dst_id=dst_id,
        properties=properties,
        acl=acl,
        system_annotations=system_annotations,
        label=label
    )


@event.listens_for(Edge, 'before_insert', propagate=True)
def receive_before_insert(mapper, connection, edge):
    edge._validate()
    edge._props = edge.properties
