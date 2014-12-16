from sqlalchemy import UniqueConstraint, String
from sqlalchemy.dialects.postgres import ARRAY, JSONB, TIMESTAMP
from sqlalchemy import Column, Integer, Text
from datetime import datetime

from sanitizer import sanitize
from psqlgraph import Base


class PsqlNode(Base):

    """Node class to represent a node entry in the postgresql table
    'nodes' inherits the SQLAlchemy Base class
    """

    __tablename__ = 'nodes'
    __table_args__ = (UniqueConstraint('node_id', name='_node_id_uc'),)

    key = Column(Integer, primary_key=True)
    node_id = Column(String(36), nullable=False)
    label = Column(Text, nullable=False)
    created = Column(TIMESTAMP, nullable=False,
                     default=sanitize(datetime.now()))
    acl = Column(ARRAY(Text))
    system_annotations = Column(JSONB, default={})
    properties = Column(JSONB, default={})

    def __repr__(self):
        return '<PsqlNode({node_id}>'.format(node_id=self.node_id)

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

    def copy(self):
        return PsqlNode(
            node_id=self.node_id,
            acl=self.acl,
            system_annotations=self.system_annotations,
            label=self.label,
            properties=self.properties,
        )

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

        .. note::
           If the new node contains an acl list, it be appended to
           the previous acl list

        The following class members cannot be updated: ``label, key, node_id``

        :param PsqlNode node: The new node to be merged onto this instance

        """

        if system_annotations:
            self.syste_annotations = self.system_annotations.update(
                sanitize(system_annotations))

        if properties:
            properties = sanitize(properties)
            self.properties.update(properties)

        if acl:
            self.acl += acl


class PsqlVoidedNode(Base):

    """Node class to represent a node entry in the postgresql table
    'nodes' inherits the SQLAlchemy Base class
    """

    __tablename__ = 'voided_nodes'

    key = Column(Integer, primary_key=True)
    node_id = Column(String(36), nullable=False)
    label = Column(Text, nullable=False)
    voided = Column(TIMESTAMP, nullable=False)
    created = Column(
        TIMESTAMP, nullable=False, default=sanitize(datetime.now()))
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
