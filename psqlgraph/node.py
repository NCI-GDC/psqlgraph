from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.postgres import ARRAY, JSONB
from sqlalchemy import Column, Text, DateTime, BigInteger
from sqlalchemy.orm import object_session
from datetime import datetime
import copy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import event, ForeignKey
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.ext.hybrid import hybrid_property


Base = declarative_base()
basic_attributes = ['_id', '_created', '_acl', '__sysan__', '_label']


def node_load_listener(*args, **kwargs):
    print 'event', args, kwargs


def sanitize(properties):
    sanitized = {}
    for key, value in properties.items():
        if isinstance(value, (int, str, long, bool, type(None))):
            sanitized[str(key)] = value
        else:
            sanitized[str(key)] = str(value)
    return sanitized


class Node(Base):

    # General node attributes
    _id = Column(
        Text,
        primary_key=True,
        nullable=False,
    )

    _created = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(),
    )

    _acl = Column(
        ARRAY(Text),
    )

    __sysan__ = Column(
        'system_annotations',
        JSONB,
        default={},
    )

    _label = Column(
        Text,
        nullable=False,
    )

    # Table properties
    __tablename__ = '_nodes'
    __table_args__ = (UniqueConstraint('_id', name='_node_id_uc'),)
    __mapper_args__ = {
        'polymorphic_on': _label,
        'polymorphic_identity': '_node',
        'with_polymorphic': '*',
    }

    def __init__(self, _id=None, label=None, acl=[],
                 sysan={}, properties={}):
        self._find_properties()
        self._sysan = sysan
        self._id = _id
        self.acl = acl
        self.label = label
        self.set_properties(properties)

    def _find_properties(self):
        cls = self.__class__
        self._properties = [k for k, v in cls.__dict__.items()
                            if type(v) == InstrumentedAttribute
                            and k not in basic_attributes]

    @hybrid_property
    def properties(self):
        if not hasattr(self, '_properties'):
            self._find_properties()
        for k in self._properties:
            getattr(self, k)
        return {k: self.__dict__[k] for k in self._properties}

    def set_sysan(self, sysan):
        self.__sysan__ = sanitize(sysan)

    @hybrid_property
    def system_annotations(self):
        return self.__sysan__

    @system_annotations.setter
    def system_annotations(self, sysan):
        self.set_sysan(sysan)

    @system_annotations.setter
    def _sysan(self, sysan):
        self.set_sysan(sysan)

    @hybrid_property
    def acl(self):
        return self._acl

    @acl.setter
    def acl(self, acl):
        self._acl = acl

    @hybrid_property
    def label(self):
        return self._label

    @label.setter
    def label(self, label):
        self._label = label

    @hybrid_property
    def node_id(self):
        return self._id

    @node_id.setter
    def node_id(self, node_id):
        self._id = node_id

    @classmethod
    def get_subclass(cls, _label):
        for c in cls.__subclasses__():
            c_label = getattr(c, '__mapper_args__', {}).get(
                'polymorphic_identity', None)
            if c_label == _label:
                return c
        raise KeyError('Node has no subclass {}'.format(_label))

    @classmethod
    def get_subclass_table_names(_label):
        return [s.__tablename__ for s in Node.__subclasses__()]

    @classmethod
    def get_subclasses(_label):
        return [s for s in Node.__subclasses__()]

    def has_property(self, key):
        cls = self.__class__
        return (hasattr(cls, key)
                and type(getattr(cls, key)) == InstrumentedAttribute
                and key not in basic_attributes)

    def set_properties(self, properties):
        for key, value in properties.items():
            if not self.has_property(key):
                raise AttributeError('{} has no attribute {}'.format(
                    type(self), key))
            setattr(self, key, value)

    def get_name(self):
        return type(self).__name__

    def is_inherited(self):
        return not type(self) == Node

    def get_session(self):
        return object_session(self)

    def __repr__(self):
        return '<{}({node_id}, {_label})>'.format(
            self.__class__.__name__, node_id=self._id, _label=self._label)

    def __getitem__(self, prop):
        return getattr(self, prop)

    def __setitem__(self, prop, val):
        setattr(self, prop, val)

    def copy(self):
        node = Node(
            _id=self._id,
            _acl=self.acl,
            _system_annotations=self.system_annotations,
            _label=self.label,
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


def PolyNode(_id=None, label=None, acl=[], sysan={}, properties={},
             node_id=None):
    assert label, 'You cannot create a PolyNode without a label.'
    if _id is None and node_id is not None:
        _id = node_id
    Type = Node.get_subclass(label)

    return Type(_id, label, acl, sysan, properties)


class PsqlNode(Node):
    pass


class VoidedNode(Base):

    # General node attributes
    _key = Column(
        BigInteger,
        primary_key=True,
    )

    _id = Column(
        Text,
        nullable=False,
    )

    _created = Column(
        DateTime(timezone=True),
        nullable=False,
    )

    _voided = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(),
    )

    _acl = Column(
        ARRAY(Text),
    )

    _sysan = Column(
        JSONB,
        default={},
    )

    _label = Column(
        Text,
        nullable=False,
    )

    properties = Column(
        JSONB,
        default={},
    )

    # Table properties
    __tablename__ = '_voided_nodes'

    def __init__(self, node):
        self._created = node._created
        self._id = node._id
        self._acl = node._acl
        self._label = node._label
        self._sysan = node._sysan
        self.set_properties(node.properties)

    def set_properties(self, properties):
        self.properties = sanitize(properties)

    def __getattr__(self, key):
        raise Exception()
        if key not in self.__dict__:
            raise AttributeError('{} has no attribute {}'.format(
                type(self), key))
        if key == 'properties':
            return {str(k): v for k, v in self.__dict__['properties']}
        else:
            return self.__dict__[key]

    def __getitem__(self, prop):
        return getattr(self, prop)


class Edge(object):

    def __init__(self, *args, **kwargs):
        pass

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def __getitem__(self, key):
        getattr(self, key)


@event.listens_for(Node, 'before_update', propagate=True)
@event.listens_for(Node, 'after_insert', propagate=True)
def node_update_listener(mapper, connection, target):
    session = target.get_session()
    voided_node = VoidedNode(target)
    session.add(voided_node)
