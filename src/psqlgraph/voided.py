"""A module for recording various voided states within a graph.

Voided entities represent the history of a graph entity recoding its past states.
"""

from __future__ import annotations

import copy
import datetime
import types
from collections.abc import Iterable, Mapping
from typing import Any, Protocol, overload

import sqlalchemy
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext import declarative, hybrid

Base = declarative.declarative_base()


class VoidedBase:
    """A common base for voided graph entities."""

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

    @hybrid.hybrid_property
    def props(self) -> dict[str, Any]:  # type: ignore
        """Alias of properties"""
        return self.properties

    @props.setter
    def props(self, properties: dict[str, Any]) -> None:
        """Alias of properties"""
        self.properties = properties

    @hybrid.hybrid_property
    def sysan(self) -> dict[str, Any]:  # type: ignore
        """Alias of system annotations"""
        return self.system_annotations

    @sysan.setter
    def sysan(self, sysan: dict[str, Any]) -> None:
        """Alias of system annotations"""
        self.system_annotations = sysan


class _Edge(Protocol):
    """An abstraction of the basic properties of an edge."""

    @property
    def created(self) -> datetime.datetime: ...

    @property
    def src_id(self) -> str: ...

    @property
    def dst_id(self) -> str: ...

    @property
    def acl(self) -> list[str]: ...

    @property
    def label(self) -> str: ...

    @property
    def system_annotations(self) -> dict[str, Any]: ...

    @property
    def properties(self) -> dict[str, Any]: ...


class VoidedEdge(VoidedBase, Base):
    """A voided edge and/or partial state of a edge."""

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

    label = sqlalchemy.Column(
        sqlalchemy.Text,
        primary_key=True,
        nullable=False,
    )

    @overload
    def __init__(
        self,
        *,
        src_id: str,
        dst_id: str,
        label: str,
        created: datetime.datetime | None = None,
        acl: Iterable[str] = (),
        system_annotations: Mapping[str, Any] = types.MappingProxyType({}),
        properties: Mapping[str, Any] = types.MappingProxyType({}),
    ) -> None:
        """A set of properties associated with an edge which is being voided.

        Args:
            src_id: The node id of the source node.
            dst_id: The node id of the destination node.
            label: The label of the edge.
            created: The time at which the edge was created.
            acl: The acl associated with the edge.
            system_annotation: The system annotations associated with the edge.
            properties: The properties of the edge.
        """
        ...

    @overload
    def __init__(
        self,
        edge: _Edge,
        /,
    ) -> None:
        """A voided edge based on the state of the given edge.

        Args:
            edge: The edge containing the state which is being voided.
        """
        ...

    def __init__(
        self,
        edge: _Edge | None = None,
        *,
        src_id: str | None = None,
        dst_id: str | None = None,
        label: str | None = None,
        created: datetime.datetime | None = None,
        acl: Iterable[str] = (),
        system_annotations: Mapping[str, Any] = types.MappingProxyType({}),
        properties: Mapping[str, Any] = types.MappingProxyType({}),
    ):
        if edge:
            self.created = edge.created
            self.src_id = edge.src_id
            self.dst_id = edge.dst_id
            self.acl = list(edge.acl)
            self.label = edge.label
            self.system_annotations = copy.deepcopy(dict(edge.system_annotations))
            self.properties = copy.deepcopy(dict(edge.properties))
        else:
            assert src_id and dst_id and label, (
                "Edge must have valid src, dst, and label to be voided."
            )

            self.created = created
            self.src_id = src_id
            self.dst_id = dst_id
            self.acl = list(acl)
            self.label = label
            self.system_annotations = copy.deepcopy(dict(system_annotations))
            self.properties = copy.deepcopy(dict(properties))


class _Node(Protocol):
    """An abstraction of the basic properties of a node."""

    @property
    def created(self) -> datetime.datetime: ...

    @property
    def node_id(self) -> str | None: ...

    @property
    def acl(self) -> list[str]: ...

    @property
    def label(self) -> str: ...

    @property
    def system_annotations(self) -> dict[str, Any]: ...

    @property
    def _props(self) -> dict[str, Any]: ...


class VoidedNode(VoidedBase, Base):
    """A voided node and/or partial state of a node."""

    __tablename__ = "_voided_nodes"

    key = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)

    node_id = sqlalchemy.Column(
        sqlalchemy.Text,
        nullable=False,
    )

    label = sqlalchemy.Column(
        sqlalchemy.Text,
        nullable=False,
    )

    @overload
    def __init__(
        self,
        *,
        node_id: str,
        label: str,
        created: datetime.datetime | None = None,
        acl: Iterable[str] = (),
        system_annotations: Mapping[str, Any] = types.MappingProxyType({}),
        properties: Mapping[str, Any] = types.MappingProxyType({}),
    ) -> None:
        """A set of properties associated with a node which is being voided.

        Args:
            node_id: The node id associated with the data being voided.
            label: The label of the node.
            created: The time at which the node was created.
            acl: The acl associated with the node.
            system_annotation: The system annotations associated with the node.
            properties: The properties of the node.
        """
        ...

    @overload
    def __init__(self, node: _Node, /) -> None:
        """A voided node based on the state of the given node.

        Args:
            node: The node containing the state which is being voided.
        """
        ...

    def __init__(
        self,
        node: _Node | None = None,
        *,
        node_id: str | None = None,
        label: str | None = None,
        created: datetime.datetime | None = None,
        acl: Iterable[str] = (),
        system_annotations: Mapping[str, Any] = types.MappingProxyType({}),
        properties: Mapping[str, Any] = types.MappingProxyType({}),
    ):
        if node:
            assert node.node_id, "Node must have valid node_id to be voided"

            self.created = node.created
            self.node_id = node.node_id
            self.acl = list(node.acl)
            self.label = node.label
            self.system_annotations = copy.deepcopy(dict(node.system_annotations))
            self.properties = copy.deepcopy(dict(node._props))

        else:
            assert node_id, "Node must have valid node_id to be voided"
            assert label, "Node must have valid label to be voided"

            self.created = created
            self.node_id = node_id
            self.acl = list(acl)
            self.label = label
            self.system_annotations = copy.deepcopy(dict(system_annotations))
            self.properties = copy.deepcopy(dict(properties))
