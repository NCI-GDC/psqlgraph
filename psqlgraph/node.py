from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.postgres import ARRAY, JSONB
from sqlalchemy import Column, Integer, Text, DateTime
from sqlalchemy.orm import relationship, object_session
from datetime import datetime
import copy

from sanitizer import sanitize
from psqlgraph import Base
from edge import PsqlEdge


def add_node_constraint(constraint):
    """Adds a constraint to edges table.  This would need to be called
    prior to creation of tables to have any effect

    :param UniqueConstraint constraint:
        The uniqueness constraint to add to the tables `nodes`

    """
    Base.metadata.tables.get('nodes').constraints.add(constraint)


class PsqlNode(Base):

    """Node class to represent a node entry in the postgresql table
    'nodes' inherits the SQLAlchemy Base class
    """

    __tablename__ = 'nodes'
    __table_args__ = (UniqueConstraint('node_id', name='_node_id_uc'),)

    key = Column(Integer, primary_key=True)
    node_id = Column(Text, nullable=False)
    label = Column(Text, nullable=False)
    created = Column(
        DateTime(timezone=True),
        nullable=False,
        default=sanitize(datetime.now())
    )
    acl = Column(ARRAY(Text))
    system_annotations = Column(JSONB, default={})
    properties = Column(JSONB, default={})
    edges_out = relationship("PsqlEdge", foreign_keys=[PsqlEdge.src_id])
    edges_in = relationship("PsqlEdge", foreign_keys=[PsqlEdge.dst_id])

    def get_session(self):
        return object_session(self)

    def __repr__(self):
        return '<PsqlNode({node_id}, {label})>'.format(
            node_id=self.node_id, label=self.label)

    def __init__(self, node_id=node_id, label=label, acl=[],
                 system_annotations={},
                 properties={}):

        system_annotations = sanitize(system_annotations)
        properties = sanitize(properties)
        self.node_id = node_id
        self.acl = acl
        self.system_annotations = system_annotations
        self.label = label
        self.properties = properties

    def __getitem__(self, prop):
        return self.properties[prop]

    def __setitem__(self, prop, val):
        self.properties[prop] = sanitize(val)

    def copy(self):
        return PsqlNode(
            node_id=self.node_id,
            acl=self.acl,
            system_annotations=self.system_annotations,
            label=self.label,
            properties=self.properties,
        )

    def get_edges(self):
        for edge_in in self.edges_in:
            yield edge_in
        for edge_out in self.edges_out:
            yield edge_out

    def get_neighbors(self):
        for edge_in in self.edges_in:
            yield edge_in.src
        for edge_out in self.edges_iout:
            yield edge_out.dst

    def walk_forward(self):
        for edge_out in self.edges_out:
            yield edge_out.dst
            for subdst in edge_out.dst.walk_forward():
                yield subdst

    def walk_backward(self):
        for edge_in in self.edges_in:
            yield edge_in.src
            for subsrc in edge_in.src.walk_backward():
                yield subsrc

    def merge(self, acl=[], system_annotations={}, properties={}):
        """Merges a new node onto this instance.  The parameter ``node``
        should contain the 'new' values with the following effects. In
        general, updates are additive. New properties will be added to
        old properties.  New system annotations will be added system
        annotations. New acl will will be added to old acl.  For
        removal of a property, system_annotation, or acl entry is
        required, see :func
        PsqlGraphDriver.node_delete_property_keys:, :func
        PsqlGraphDriver.node_delete_system_annotation_keys:, :func
        PsqlGraphDriver.node_remove_acl_item:

        .. note:: If the new node contains an acl, the previous acl is
           replaced by the new one

        The following class members cannot be updated: ``label, key, node_id``

        :param PsqlNode node: The new node to be merged onto this instance

        """

        if system_annotations:
            self.system_annotations = copy.deepcopy(self.system_annotations)
            self.system_annotations.update(sanitize(system_annotations))

        if properties:
            self.properties = copy.deepcopy(self.properties)
            self.properties.update(sanitize(properties))

        if acl:
            self.acl = acl


class Node(PsqlNode):
    pass


class PsqlVoidedNode(Base):

    """Node class to represent a node entry in the postgresql table
    'nodes' inherits the SQLAlchemy Base class
    """

    __tablename__ = 'voided_nodes'

    key = Column(Integer, primary_key=True)
    node_id = Column(Text, nullable=False)
    label = Column(Text, nullable=False)
    voided = Column(
        DateTime(timezone=True),
        nullable=False,
        default=sanitize(datetime.now())
    )
    created = Column(
        DateTime(timezone=True),
        nullable=False,
        default=sanitize(datetime.now())
    )
    acl = Column(ARRAY(Text))
    system_annotations = Column(JSONB, default={})
    properties = Column(JSONB, default={})

    def __repr__(self):
        return '<PsqlVoidedNode({node_id})>'.format(node_id=self.node_id)

    def __init__(self, node, voided=datetime.now()):
        self.node_id = node.node_id
        self.acl = node.acl
        self.system_annotations = sanitize(node.system_annotations)
        self.label = node.label
        self.properties = sanitize(node.properties)
        self.voided = datetime.now()
