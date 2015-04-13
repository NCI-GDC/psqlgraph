from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.postgres import ARRAY, JSONB
from sqlalchemy import Column, Text, DateTime
from sqlalchemy.orm import object_session
from datetime import datetime
import copy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import event


Base = declarative_base()


def node_load_listener(target, context):
    pass


class Node(Base):

    __tablename__ = '_nodes'
    __table_args__ = (UniqueConstraint('_id', name='_node_id_uc'),)

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

    _sysan = Column(
        JSONB,
        default={},
    )

    _label = Column(
        Text,
        nullable=False,
    )

    __mapper_args__ = {
        'polymorphic_on': _label,
        'polymorphic_identity': '_node',
        'with_polymorphic': '*',
    }

    def __init__(self, _id=None, label=None, acl=[],
                 sysan={}, properties={}):
        self._sysan = sysan
        self._id = _id
        self._acl = acl
        self._label = label
        self.set_properties(properties)

    def __getattr__(self, key):
        if key == 'properties':
            assert hasattr(self, '_properties'),\
                'Please add _properties key list to {}'.format(type(self))
            return {k: self.__dict__[k] for k in self._properties}
        else:
            if not hasattr(self, key):
                raise AttributeError('{} has no attribute {}'.format(
                    type(self), key))
            return self.__dict__[key]

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

    def set_properties(self, properties):
        for key, value in properties.items():
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
    if _id is None and node_id is not None:
        _id = node_id
    Type = Node.get_subclass(label)

    return Type(_id, label, acl, sysan, properties)


class PsqlNode(Node):
    pass

event.listen(Node, 'load', node_load_listener, propagate=True)
