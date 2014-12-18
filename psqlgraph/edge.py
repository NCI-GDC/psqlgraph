from sqlalchemy.orm import relationship
from sqlalchemy import UniqueConstraint, ForeignKey
from sqlalchemy.dialects.postgres import JSONB, TIMESTAMP
from sqlalchemy import Column, Integer, Text
from datetime import datetime

from sanitizer import sanitize
from psqlgraph import Base


class PsqlEdge(Base):

    """Edge class to represent a edge entry in the postgresql table
    'edges' inherits the SQLAlchemy Base class.

    See tools/setup_psqlgraph script for details on table setup.
    """

    __tablename__ = 'edges'
    __table_args__ = (UniqueConstraint('src_id', 'dst_id', name='_edge_uc'),)

    key = Column(Integer, primary_key=True)
    src_id = Column(Text, ForeignKey('nodes.node_id'), nullable=False)
    dst_id = Column(Text, ForeignKey('nodes.node_id'), nullable=False)
    created = Column(
        TIMESTAMP, nullable=False, default=sanitize(datetime.now())
    )
    system_annotations = Column(JSONB, default={})
    label = Column(Text, nullable=False)
    properties = Column(JSONB, default={})

    src = relationship("PsqlNode", foreign_keys=[src_id])
    dst = relationship("PsqlNode", foreign_keys=[dst_id])

    def __init__(self, src_id=src_id, dst_id=dst_id, label=label,
                 system_annotations={},
                 properties={}):

        system_annotations = sanitize(system_annotations)
        properties = sanitize(properties)
        self.src_id = src_id
        self.dst_id = dst_id
        self.system_annotations = system_annotations
        self.label = label
        self.properties = properties

    def __repr__(self):
        return '<PsqlEdge(({src_id})->({dst_id}))>'.format(
            src_id=self.src_id, dst_id=self.dst_id)

    def merge(self, system_annotations={}, properties={}):
        """Merges a new edge onto this instance.  The parameter ``edge``
        should contain the 'new' values with the following effects. In
        general, updates are additive. New properties will be added to
        old properties.  New system annotations will be added system
        annotations. For removal of a property or system_annotation
        entry is required, see :func:
        PsqlGraphDriver.edge_delete_property_keys, :func:
        PsqlGraphDriver.edge_delete_system_annotation_keys

        .. note::
            If the new edge contains an acl list, it be appended
            to the previous acl list

        The following class members cannot be updated: ``label, key,
        src_id, dst_id``

        :param PsqlEdge edge: The new edge to be merged onto this instance

        """

        if system_annotations:
            self.syste_annotations = self.system_annotations.update(
                sanitize(system_annotations))

        if properties:
            properties = sanitize(properties)
            self.properties.update(properties)


class PsqlVoidedEdge(Base):

    """Edge class to represent a edge entry in the postgresql table
    'edges' inherits the SQLAlchemy Base class.

    See tools/setup_psqlgraph script for details on table setup.
    """

    __tablename__ = 'voided_edges'

    key = Column(Integer, primary_key=True)
    src_id = Column(Text, nullable=False)
    dst_id = Column(Text, nullable=False)
    voided = Column(TIMESTAMP, nullable=False)
    created = Column(TIMESTAMP, nullable=False,
                     default=sanitize(datetime.now()))
    system_annotations = Column(JSONB, default={})
    label = Column(Text, nullable=False)
    properties = Column(JSONB, default={})

    def __init__(self, edge, voided=datetime.now()):
        self.src_id = edge.src_id
        self.dst_id = edge.dst_id
        self.system_annotations = sanitize(edge.system_annotations)
        self.label = edge.label
        self.properties = sanitize(edge.properties)
        self.voided = voided

    def __repr__(self):
        return '<PsqlEdge(({src_id})->({dst_id}))>'.format(
            src_id=self.src_id, dst_id=self.dst_id)
