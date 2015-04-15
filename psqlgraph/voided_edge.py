from sqlalchemy.dialects.postgres import ARRAY, JSONB
from sqlalchemy import Column, Text, DateTime, BigInteger
from datetime import datetime
from base import Base


class VoidedEdge(Base):

    __tablename__ = '_voided_edges'

    key = Column(
        BigInteger,
        primary_key=True,
        nullable=False
    )

    src_id = Column(
        Text,
        nullable=False,
    )

    dst_id = Column(
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

    def __init__(self, edge):
        self.created = edge.created
        self.src_id = edge.src_id
        self.dst_id = edge.dst_id
        self.acl = edge.acl
        self.label = edge.label
        self.system_annotations = edge.system_annotations
        self.properties = edge.properties
