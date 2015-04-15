from datetime import datetime
from sqlalchemy import Column, Text, DateTime, UniqueConstraint, event
from sqlalchemy.dialects.postgres import ARRAY, JSONB
from sqlalchemy.ext.declarative import AbstractConcreteBase, declared_attr
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import object_session, sessionmaker
import copy

from base import Base
from voided_node import VoidedNode


def sanitize(properties):
    sanitized = {}
    for key, value in properties.items():
        if isinstance(value, (int, str, long, bool, type(None))):
            sanitized[str(key)] = value
        elif isinstance(value, unicode):
            sanitized[str(key)] = str(value)
        else:
            raise ValueError(
                'Cannot serialize {} to JSONB property'.format(type(value)))
    return sanitized


class SystemAnnotationDict(dict):
    """Transparent wrapper for system annotations so you can update it as
    if it were a dict and the changes get pushed to the sqlalchemy object

    """

    def __init__(self, source):
        self.source = source
        super(SystemAnnotationDict, self).__init__(source._sysan)

    def update(self, system_annotations):
        system_annotations = sanitize(system_annotations)
        temp = sanitize(self.source._sysan)
        temp.update(system_annotations)
        self.source._sysan = temp
        super(SystemAnnotationDict, self).update(system_annotations)

    def __setitem__(self, key, val):
        self.update({key: val})


class PropertiesDict(dict):

    def __init__(self, source):
        self.source = source
        super(PropertiesDict, self).__init__(
            source.property_template(source._props))

    def update(self, properties):
        properties = sanitize(properties)
        temp = sanitize(self.source._props)
        temp.update(properties)
        self.source._props = temp
        super(PropertiesDict, self).update(temp)

    def __setitem__(self, key, val):
        self.update({key: val})


class Node(AbstractConcreteBase, Base):

    node_id = Column(
        Text,
        primary_key=True,
        nullable=False,
    )

    created = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(),
    )

    acl = Column(
        ARRAY(Text),
        default=list(),
    )

    _sysan = Column(
        JSONB,
        default={},
    )

    _props = Column(
        JSONB,
        default={},
    )

    _label = Column(
        Text,
        nullable=False,
    )

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    @declared_attr
    def __mapper_args__(cls):
        name = cls.__name__.lower()
        if name == 'node':
            return {
                'polymorphic_on': cls._label,
                'polymorphic_identity': 'node',
                'with_polymorphic': '*',
            }
        else:
            return {
                'polymorphic_identity': name,
                'concrete': True,
            }

    @declared_attr
    def __table_args__(cls):
        name = cls.__name__.lower()
        if name == 'voidednode':
            return ('extend_existing')
        else:
            return (UniqueConstraint(
                'node_id', name='_{}_id_uc'.format(name)),)

    def __init__(self, node_id=None, properties={}, acl=[],
                 system_annotations={}, label=None):
        self._props = {}
        self.system_annotations = system_annotations
        self.node_id = node_id
        self.acl = acl
        self.label = label or self.__mapper_args__['polymorphic_identity']
        self.properties = properties

    @hybrid_property
    def properties(self):
        return PropertiesDict(self)

    @properties.setter
    def properties(self, properties):
        for key, val in sanitize(properties).items():
            self.set_property(key, val)

    def set_property(self, key, val):
        if not self.has_property(key):
            raise KeyError('{} has no property {}'.format(type(self), key))
        self.properties[key] = val

    def property_template(self, properties={}):
        temp = {k: None for k in self.get_property_list()}
        temp.update(properties)
        return temp

    @hybrid_property
    def label(self):
        return self._label

    @label.setter
    def label(self, label):
        if self._label is not None and self._label != label:
            raise AttributeError('Cannot change label from {} to {}'.format(
                self._label, label))
        self._label = label

    @hybrid_property
    def system_annotations(self):
        return SystemAnnotationDict(self)

    @system_annotations.setter
    def system_annotations(self, sysan):
        self._sysan = sanitize(sysan)

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
    def get_subclasses(label):
        return [s for s in Node.__subclasses__()]

    @classmethod
    def get_property_list(cls):
        return [attr for attr in dir(cls)
                if attr in cls.__dict__
                and isinstance(cls.__dict__[attr], hybrid_property)]

    def has_property(self, key):
        return key in self.get_property_list()

    def get_name(self):
        return type(self).__name__

    def is_inherited(self):
        return not type(self) == Node

    def get_session(self):
        return object_session(self)

    def __repr__(self):
        return '<{}({node_id}, {label})>'.format(
            self.__class__.__name__, node_id=self.node_id, label=self.label)

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, val):
        self.properties[key] = val

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

    @property
    def _history(self):
        session = self.get_session()
        if not session:
            raise RuntimeError(
                '{} not bound to a session. Try get_history(session).'.format(
                    self))
        return self.get_history(session)

    def get_history(self, session):
        return session.query(VoidedNode)\
                      .filter(VoidedNode.node_id == self.node_id)\
                      .order_by(VoidedNode.voided.desc())


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


class Edge(object):

    def __init__(self, *args, **kwargs):
        pass

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def __getitem__(self, key):
        getattr(self, key)


@event.listens_for(Node, 'before_insert', propagate=True)
def receive_before_insert(mapper, connection, node):
    node._props = node.properties


def snapshot_node(session, target):
    if not target:
        return
    voided_node = VoidedNode(target)
    session.add(voided_node)


def lookup_existing_node(session, node_id):
    Clean = sessionmaker()
    Clean.configure(bind=session.bind)
    clean = Clean()
    return clean.query(Node).filter(Node.node_id == node_id).scalar()


def merge_node_onto_existing_node(session, existing, node):
    if not existing:
        node._props = node.properties
    else:
        temp = node.property_template()
        temp.update(existing._props)
        temp.update(node._props)
        node._props = temp


def receive_before_flush(session, flush_context, instances):
    for node in session.dirty:
        existing = lookup_existing_node(session, node.node_id)
        snapshot_node(session, existing)
        merge_node_onto_existing_node(session, existing, node)
