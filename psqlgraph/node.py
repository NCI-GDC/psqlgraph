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
basic_attributes = ['node_id', 'created', 'acl', '_sysan', 'label']


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


class SystemAnnotationDict(dict):
    """Transparent wrapper for system annotations so you can update it as
    if it were a dict and the changes get pushed to the sqlalchemy object

    """

    def __init__(self, source):
        self.source = source
        super(SystemAnnotationDict, self).__init__(source._sysan)

    def update(self, system_annotations):
        temp = sanitize({k: v for k, v in self.source._sysan.items()})
        temp.update(system_annotations)
        self.source._sysan = temp

    def __setitem__(self, key, val):
        self.update({key: val})


class PropertiesDict(object):
    """Object that represents a sqlalchemy model's 'properties' which are
    really just attributes now so you can update it as if it were a
    dict and the changes get pushed to the sqlalchemy object

    """

    def __init__(self, source):
        self.source = source
        self.properties = self.get_properties()

    def get_properties(self):
        if not hasattr(self.source, '_properties'):
            self.source._find_properties()
        return {k: getattr(self.source, k) for k in self.source._properties}

    def update(self, properties):
        temp = self.get_properties()
        temp.update(properties)
        self.properties.update(properties)
        self.source.set_properties(temp)

    def __setitem__(self, key, val):
        self.source.set_property(key, val)

    def __getitem__(self, key):
        return getattr(self.source, key)

    def __copy__(self):
        return copy(self.properties)

    def items(self):
        return self.properties.items()

    def iteritems(self):
        for item in self.properties.iteritems():
            yield item

    def __contains__(self, key):
        return key in self.properties

    def __eq__(self, other):
        for k, v in other.items():
            if k not in self.properties or \
               self.properties[k] != v:
                return False
        for k, v in self.properties.items():
            if k not in other or other[k] != v:
                return False
        return True

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return str(self.properties)


class Node(Base):

    # General node attributes
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

    # System annotations are wrapped under a hybrid property so we can
    # intercept interactions to allow correct setting and getting
    _sysan = Column(
        'system_annotations',
        JSONB,
        default={},
    )

    label = Column(
        Text,
        nullable=False,
    )

    # Table properties
    __tablename__ = '_nodes'
    __table_args__ = (UniqueConstraint('node_id', name='_node_id_uc'),)
    __mapper_args__ = {
        'polymorphic_on': label,
        'polymorphic_identity': '_node',
        'with_polymorphic': '*',
    }

    def __init__(self, node_id=None, label=None, acl=[],
                 sysan={}, properties={}):
        self._find_properties()
        self.system_annotations = sysan
        self.node_id = node_id
        self.acl = acl
        self.label = label or self.__mapper_args__['polymorphic_identity']
        self.set_properties(properties)

    def _find_properties(self):
        cls = self.__class__
        self._properties = [k for k, v in cls.__dict__.items()
                            if type(v) == InstrumentedAttribute
                            and k not in basic_attributes]

    @hybrid_property
    def properties(self):
        return PropertiesDict(self)

    @properties.setter
    def properties(self, properties):
        self.set_properties(properties)

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

    def set_property(self, key, val):
        if not self.has_property(key):
            raise AttributeError('{} has no attribute {}'.format(
                type(self), key))
        setattr(self, key, val)

    def get_name(self):
        return type(self).__name__

    def is_inherited(self):
        return not type(self) == Node

    def get_session(self):
        return object_session(self)

    def __repr__(self):
        return '<{}({node_id}, {label})>'.format(
            self.__class__.__name__, node_id=self.node_id, label=self.label)

    def __getitem__(self, prop):
        return getattr(self, prop)

    def __setitem__(self, prop, val):
        self.set_property(prop, val)

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


def PolyNode(node_id=None, label=None, acl=[], system_annotations={},
             properties={}):
    assert label, 'You cannot create a PolyNode without a label.'
    Type = Node.get_subclass(label)
    return Type(node_id, label, acl, system_annotations, properties)


class PsqlNode(Node):
    pass


class VoidedNode(Base):

    # General node attributes
    _key = Column(
        BigInteger,
        primary_key=True,
    )

    node_id = Column(
        Text,
        nullable=False,
    )

    created = Column(
        DateTime(timezone=True),
        nullable=False,
    )

    voided = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(),
    )

    acl = Column(
        ARRAY(Text),
    )

    system_annotations = Column(
        JSONB,
        default={},
    )

    label = Column(
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
        self.created = node.created
        self.node_id = node.node_id
        self.acl = node.acl
        self.label = node.label
        self.system_annotations = node.system_annotations
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
