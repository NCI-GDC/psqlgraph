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
    if target.is_inherited():
        target._load_properties()


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
        'polymorphic_identity': 'node',
        'with_polymorphic': '*',
    }

    def __init__(self, _id=_id, _label=None, acl=[],
                 system_annotations={}, properties={}):
        self.__class__ = self.get_subclass(_label)
        self._label = _label if _label else self.__class__.__name__
        self._sysan = system_annotations
        self._id = _id
        self._acl = acl
        self._label = _label
        self.set_properties(properties)
        if self.is_inherited():
            self._load_properties()

    def get_subclass(self, _label):
        if self.is_inherited():
            return type(self)
        for c in type(self).__subclasses__():
            c_label = getattr(c, '__mapper_args__', {}).get(
                'polymorphic_identity', None)
            if c_label == _label:
                return c
        raise KeyError('Node has no subclass {}'.format(_label))

    def _load_properties(self):
        assert hasattr(self, '_properties'),\
            'Please add _properties attribute to model {}'.format(
                type(self).__name__)
        self.__safe_set_attr__('properties', {
            k: getattr(self, k) for k in self._properties})

    def __safe_set_attr__(self, name, value):
        self.__dict__[name] = value

    def __setattr__(self, name, value):
        self.__safe_set_attr__(name, value)
        self._load_properties()

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


class PsqlNode(Node):
    pass

event.listen(Node, 'load', node_load_listener, propagate=True)
