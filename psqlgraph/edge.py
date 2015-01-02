from sqlalchemy.orm import relationship
from sqlalchemy import ForeignKey
from sqlalchemy.dialects.postgres import JSONB, TIMESTAMP
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy import Column, Integer, Text
from datetime import datetime
from sanitizer import sanitize
from psqlgraph import Base
from exc import EdgeCreationError

# Attempt to load the __table_args__ variable set by an external
# module before loading the declaring PsqlEdge.
#
# TODO: Come up with a better way than using __builtin__
import __builtin__
try:
    __builtin__.__psqlgraph_edges_table_args__
except AttributeError:
    __builtin__.__psqlgraph_edges_table_args__ = None


class PsqlEdge(Base):

    """Edge class to represent a edge entry in the postgresql table
    'edges' inherits the SQLAlchemy Base class.

    See tools/setup_psqlgraph script for details on table setup.

    In order to add a constraint set the attribute
    ``__psqlgraph_edges_table_args__`` in python's ``__builtin__``
    before loading the psqlgraph module as such:
    ```
    import __builtin__
    from sqlalchemy import UniqueConstraint
    __builtin__.__psqlgraph_edges_table_args__ = (
        UniqueConstraint('src_id', 'dst_id', 'label', name='_edge_uc'),
    )
    ````

    """

    __tablename__ = 'edges'

    @declared_attr
    def __table_args__(cls):
        return __builtin__.__psqlgraph_edges_table_args__

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
