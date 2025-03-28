"""A module for poly edges/nodes which generate an entity based on a label."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from psqlgraph import graph


def PolyEdge(
    *,
    label: str,
    src_id: str | None = None,
    dst_id: str | None = None,
    acl: list[str] | None = None,
    system_annotations: Mapping[str, Any] | None = None,
    properties: Mapping[str, Any] | None = None,
):
    edge_cls = graph.Edge.get_subclass(label)

    if not edge_cls:
        raise ValueError(f"Cannot resolve edge type with label: {label}")

    return edge_cls(
        src_id=src_id,
        dst_id=dst_id,
        properties=properties or {},
        acl=acl or [],
        system_annotations=system_annotations or {},
    )


def PolyNode(
    *,
    label: str,
    node_id: str | None = None,
    acl: list[str] | None = None,
    system_annotations: Mapping[str, Any] | None = None,
    properties: Mapping[str, Any] | None = None,
):
    node_cls = graph.Node.get_subclass(label)

    if not node_cls:
        raise ValueError(f"Cannot resolve node type with label: {label}")

    return node_cls(
        node_id=node_id,
        properties=properties or {},
        acl=acl or [],
        system_annotations=system_annotations or {},
    )
