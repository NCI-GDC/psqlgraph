import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext import hybrid

VoidedBase = orm.declarative_base()


class VoidedEdge(VoidedBase):

    __tablename__ = "_voided_edges"

    key = sqlalchemy.Column(
        sqlalchemy.BigInteger, primary_key=True, nullable=False, autoincrement=True
    )

    src_id = sqlalchemy.Column(
        sqlalchemy.Text,
        primary_key=True,
        nullable=False,
    )

    dst_id = sqlalchemy.Column(
        sqlalchemy.Text,
        primary_key=True,
        nullable=False,
    )

    created = sqlalchemy.Column(
        sqlalchemy.DateTime(timezone=True),
        nullable=False,
        server_default=sqlalchemy.text("now()"),
    )

    voided = sqlalchemy.Column(
        sqlalchemy.DateTime(timezone=True),
        nullable=False,
        server_default=sqlalchemy.text("now()"),
    )

    acl = sqlalchemy.Column(
        sqlalchemy.ARRAY(sqlalchemy.Text),
        default=list(),
    )

    system_annotations = sqlalchemy.Column(
        postgresql.JSONB,
        default={},
    )

    properties = sqlalchemy.Column(
        postgresql.JSONB,
        default={},
    )

    props = hybrid.hybrid_property(
        lambda self: self.properties, lambda self, value: setattr(self, "properties", value)
    )

    sysan = hybrid.hybrid_property(
        lambda self: self.system_annotations,
        lambda self, value: setattr(self, "system_annotations", value),
    )

    label = sqlalchemy.Column(
        sqlalchemy.Text,
        primary_key=True,
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


class VoidedNode(VoidedBase):

    __tablename__ = "_voided_nodes"

    key = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)

    node_id = sqlalchemy.Column(
        sqlalchemy.Text,
        nullable=False,
    )

    created = sqlalchemy.Column(
        sqlalchemy.DateTime(timezone=True),
        nullable=False,
        server_default=sqlalchemy.text("now()"),
    )

    voided = sqlalchemy.Column(
        sqlalchemy.DateTime(timezone=True),
        nullable=False,
        server_default=sqlalchemy.text("now()"),
    )

    acl = sqlalchemy.Column(
        sqlalchemy.ARRAY(sqlalchemy.Text),
        default=list(),
    )

    system_annotations = sqlalchemy.Column(
        postgresql.JSONB,
        default={},
    )

    properties = sqlalchemy.Column(
        postgresql.JSONB,
        default={},
    )

    props = hybrid.hybrid_property(
        lambda self: self.properties, lambda self, value: setattr(self, "properties", value)
    )

    sysan = hybrid.hybrid_property(
        lambda self: self.system_annotations,
        lambda self, value: setattr(self, "system_annotations", value),
    )

    label = sqlalchemy.Column(
        sqlalchemy.Text,
        nullable=False,
    )

    def __init__(self, node) -> None:
        self.created = node.created
        self.node_id = node.node_id
        self.acl = node.acl
        self.label = node.label
        self.system_annotations = node.system_annotations
        self.properties = node._props
