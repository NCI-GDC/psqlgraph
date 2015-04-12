from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.postgres import ARRAY, JSONB
from sqlalchemy import Column, Text, DateTime
from sqlalchemy.orm import object_session
from datetime import datetime
import copy
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class Node(Base):
    _properties = []
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

        assert hasattr(self, '_properties'), \
            'Please define _properties attr in your model'

        self._label = self.__class__.__name__.lower()
        self._sysan = system_annotations
        self._id = _id
        self._acl = acl
        self._label = _label
        for key, value in properties.items():
            setattr(self, key, value)
        self.properties = {k: getattr(self, k) for k in self._properties}

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
