from datetime import datetime
from sqlalchemy import Column, Text, DateTime, UniqueConstraint, event
from sqlalchemy.dialects.postgres import ARRAY, JSONB
from sqlalchemy.ext.declarative import AbstractConcreteBase, declared_attr
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import object_session, sessionmaker

from base import ORMBase
from util import sanitize


class Edge(ORMBase):

    src_id = Column(
        Text,
        primary_key=True,
        nullable=False,
    )

    dst_id = Column(
        Text,
        primary_key=True,
        nullable=False,
    )

    def __init__(self, *args, **kwargs):
        pass
