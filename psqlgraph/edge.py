from sqlalchemy.orm import relationship
from sqlalchemy import ForeignKey
from sqlalchemy.dialects.postgres import JSONB
from sqlalchemy import Column, Integer, Text, DateTime
from datetime import datetime
from sanitizer import sanitize
from psqlgraph import Base
from exc import EdgeCreationError
import uuid


def add_edge_constraint(constraint):
    """Adds a constraint to edges table.  This would need to be called
    prior to creation of tables to have any effect

    :param UniqueConstraint constraint:
        The uniqueness constraint to add to the tables `edges`

    """
    Base.metadata.tables.get('edges').constraints.add(constraint)


class PsqlEdge(Base):

    """Edge class to represent a edge entry in the postgresql table
    'edges' inherits the SQLAlchemy Base class.

    See tools/setup_psqlgraph script for details on table setup.

    To add constraints or override other __table_args__:
    ```
    from sqlalchemy import UniqueConstraint
    from psqlgraph.edge import PsqlEdge, edges_add_table_arg
    edges_add_table_arg(UniqueConstraint(
        PsqlEdge.src_id, PsqlEdge.dst_id, PsqlEdge.label))
    ```
    """

    __tablename__ = 'edges'
    __table_args__ = None

    key = Column(Integer, primary_key=True)
    edge_id = Column(Text, nullable=False, default=str(uuid.uuid4()))
    src_id = Column(
        Text,
        ForeignKey('nodes.node_id', deferrable=True, initially="DEFERRED"),
        nullable=False
    )
    dst_id = Column(
        Text,
        ForeignKey('nodes.node_id', deferrable=True, initially="DEFERRED"),
        nullable=False
    )
    created = Column(
        DateTime(timezone=True),
        nullable=False,
        default=sanitize(datetime.now())
    )
    system_annotations = Column(JSONB, default={})
    label = Column(Text, nullable=False)
    properties = Column(JSONB, default={})

    src = relationship("PsqlNode", foreign_keys=[src_id])
    dst = relationship("PsqlNode", foreign_keys=[dst_id])

    def __init__(self, src_id, dst_id, label,
                 system_annotations={},
                 properties={}):

        if label is None:
            raise EdgeCreationError(
                'Illegal edge creation without label')

        system_annotations = sanitize(system_annotations)
        properties = sanitize(properties)
        self.src_id = src_id
        self.dst_id = dst_id
        self.system_annotations = system_annotations
        self.label = label
        self.properties = properties

    def __repr__(self):
        return '<PsqlEdge(({src_id})-[{label}]->({dst_id}))>'.format(
            src_id=self.src_id, label=self.label, dst_id=self.dst_id)

    def __getitem__(self, prop):
        return self.properties[prop]

    def __setitem__(self, prop, val):
        self.properties[prop] = sanitize(val)

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
            self.system_annotations.update(
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
    edge_id = Column(Text, nullable=False)
    src_id = Column(Text, nullable=False)
    dst_id = Column(Text, nullable=False)
    voided = Column(
        DateTime(timezone=True),
        nullable=False,
        default=sanitize(datetime.now()),
    )
    created = Column(
        DateTime(timezone=True),
        nullable=False,
        default=sanitize(datetime.now())
    )
    system_annotations = Column(JSONB, default={})
    label = Column(Text, nullable=False)
    properties = Column(JSONB, default={})

    def __init__(self, edge, voided=datetime.now()):
        self.edge_id = edge.edge_id
        self.src_id = edge.src_id
        self.dst_id = edge.dst_id
        self.system_annotations = sanitize(edge.system_annotations)
        self.label = edge.label
        self.properties = sanitize(edge.properties)
        self.voided = voided

    def __repr__(self):
        return '<PsqlEdge(({src_id})-[{label}]->({dst_id}))>'.format(
            src_id=self.src_id, label=self.label, dst_id=self.dst_id)
