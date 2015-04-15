from sqlalchemy.dialects.postgres import ARRAY, JSONB
from sqlalchemy import Column, Text, DateTime, BigInteger
from datetime import datetime
from base import Base


class VoidedNode(Base):

    __tablename__ = '_voided_nodes'

    key = Column(
        BigInteger,
        primary_key=True,
        nullable=False
    )

    node_id = Column(
        Text,
        nullable=False,
    )

    created = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(),
    )

    voided = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(),
    )

    acl = Column(
        ARRAY(Text),
        default=list(),
    )

    system_annotations = Column(
        JSONB,
        default={},
    )

    properties = Column(
        JSONB,
        default={},
    )

    label = Column(
        Text,
        nullable=False,
    )

    def __init__(self, node):
        self.created = node.created
        self.node_id = node.node_id
        self.acl = node.acl
        self.label = node.label
        self.system_annotations = node.system_annotations
        self.properties = node.properties
