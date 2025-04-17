"""A module for poly edges/nodes which generate an entity based on a label."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from typing_extensions import deprecated

from psqlgraph import graph


def poly_edge(
    *,
    label: str,
    src_id: str | None = None,
    dst_id: str | None = None,
    acl: list[str] | None = None,
    system_annotations: Mapping[str, Any] | None = None,
    properties: Mapping[str, Any] | None = None,
) -> graph.Edge:
    """Creates an edge with the type represented with the given label.

    Args:
        label: The label which represents the underlying edge class which should be
            instantiated.
        src_id: The ID of the source node for the edge.
        dst_id: The ID of the destination node for the edge.
        acl: The acl associated with the edge.
        system_annotations: Any system annotations which should be set on the new edge.
        properties: Any properties which should be set on the new edge.

    Returns:
        A new instance of the edge represented by the given label.
    """
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


@deprecated("An alias for `psqlgraph.poly_edge`; please call directly.")
def PolyEdge(
    *,
    label: str,
    src_id: str | None = None,
    dst_id: str | None = None,
    acl: list[str] | None = None,
    system_annotations: Mapping[str, Any] | None = None,
    properties: Mapping[str, Any] | None = None,
) -> graph.Edge:
    return poly_edge(
        label=label,
        src_id=src_id,
        dst_id=dst_id,
        acl=acl,
        system_annotations=system_annotations,
        properties=properties,
    )


def poly_node(
    *,
    label: str,
    node_id: str | None = None,
    acl: list[str] | None = None,
    system_annotations: Mapping[str, Any] | None = None,
    properties: Mapping[str, Any] | None = None,
) -> graph.Node:
    """Creates an node with the type represented with the given label.

    Args:
        label: The label which represents the underlying node class which should be
            instantiated.
        dst_id: The ID of the new node.
        acl: The acl associated with the node.
        system_annotations: Any system annotations which should be set on the new node.
        properties: Any properties which should be set on the new node.

    Returns:
        A new instance of the node represented by the given label.
    """

    node_cls = graph.Node.get_subclass(label)

    if not node_cls:
        raise ValueError(f"Cannot resolve node type with label: {label}")

    return node_cls(
        node_id=node_id,
        properties=properties or {},
        acl=acl or [],
        system_annotations=system_annotations or {},
    )


@deprecated("An alias for `psqlgraph.poly_node`; please call directly.")
def PolyNode(
    *,
    label: str,
    node_id: str | None = None,
    acl: list[str] | None = None,
    system_annotations: Mapping[str, Any] | None = None,
    properties: Mapping[str, Any] | None = None,
) -> graph.Node:
    return poly_node(
        label=label,
        node_id=node_id,
        acl=acl,
        system_annotations=system_annotations,
        properties=properties,
    )
