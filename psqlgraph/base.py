from datetime import datetime
from sqlalchemy import Column, Text, DateTime, UniqueConstraint, event, text
from sqlalchemy.dialects.postgres import ARRAY, JSONB
from sqlalchemy.ext.declarative import AbstractConcreteBase, declared_attr
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import object_session, sessionmaker
import copy

from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

from attributes import PropertiesDict, SystemAnnotationDict
from util import sanitize

abstract_classes = ['node', 'edge', 'base']


class CommonBase(object):

    created = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text('now()'),
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

    _label = Column(
        Text,
        nullable=False,
    )

    @classmethod
    def get_label(cls):
        return getattr(cls, '__label__', cls.__name__.lower())

    @declared_attr
    def __tablename__(cls):
        return cls.get_label()

    @declared_attr
    def __mapper_args__(cls):
        name = cls.get_label()
        if name in abstract_classes:
            return {
                'polymorphic_on': cls._label,
                'polymorphic_identity': name,
                'with_polymorphic': '*',
            }
        else:
            return {
                'polymorphic_identity': name,
                'concrete': True,
            }

    def __init__(self, *args, **kwargs):
        raise NotImplemented()

    @hybrid_property
    def properties(self):
        return PropertiesDict(self)

    @properties.setter
    def properties(self, properties):
        for key, val in sanitize(properties).items():
            self._set_property(key, val)

    def _set_property(self, key, val):
        if not self.has_property(key):
            raise KeyError('{} has no property {}'.format(type(self), key))
        self._props = {k: v for k, v in self._props.iteritems()}
        self._props[key] = val

    def _get_property(self, key):
        if not self.has_property(key):
            raise KeyError('{} has no property {}'.format(type(self), key))
        return self._props.get(key, None)

    def property_template(self, properties={}):
        temp = {k: None for k in self.get_property_list()}
        temp.update(properties)
        return temp

    @hybrid_property
    def label(self):
        return self._label

    @label.setter
    def label(self, label):
        if not isinstance(self.label, Column)\
           and self._label is not None\
           and self._label != label:
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
    def get_property_list(cls):
        return [attr for attr in dir(cls)
                if attr in cls.__dict__
                and isinstance(cls.__dict__[attr], hybrid_property)]

    def has_property(self, key):
        return key in self.get_property_list()

    def get_name(self):
        return type(self).__name__

    def get_session(self):
        return object_session(self)

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, val):
        setattr(self, key, val)

    def _validate(self, session=None):
        for key in getattr(self, '__nonnull_properties__', []):
            assert self.properties[key] is not None,\
                'Key {} violates non-null constraint for {}'.format(key, self)


ORMBase = declarative_base(cls=CommonBase)


def create_all(engine):
    ORMBase.metadata.create_all(engine)
    Base.metadata.create_all(engine)
