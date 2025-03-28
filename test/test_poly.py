from __future__ import annotations

from collections.abc import Callable

import pytest

import psqlgraph
from test import models


def test__poly_edge__generates_defaults() -> None:
    edge = psqlgraph.PolyEdge(label=models.Edge2.get_label())

    assert edge.src_id is None
    assert edge.dst_id is None
    assert edge.acl == []
    assert edge.system_annotations == {}
    assert edge.properties == {}


@pytest.mark.parametrize(
    "inputs",
    (
        pytest.param(
            {
                "label": models.Edge1.get_label(),
                "src_id": "s0",
                "dst_id": "d0",
                "acl": ["open"],
                "properties": {"key1": "opens a fun door somewhere"},
            },
            id="edge-with-properties",
        ),
        pytest.param(
            {
                "label": models.Edge3.get_label(),
                "src_id": "n0",
                "dst_id": "n1",
                "acl": ["secret"],
                "system_annotations": {"code": "red"},
            },
            id="edge-with-system-annotation",
        ),
    ),
)
def test__poly_edge__generates_from_input(inputs: dict) -> None:
    """Given a PolyEdge or PolyNode
    When called with an unknown label (i.e. associated with no graph entity.)
    Then a ValueError is raised.
    """
    edge = psqlgraph.PolyEdge(**inputs)

    assert edge.src_id == inputs["src_id"]
    assert edge.dst_id == inputs["dst_id"]
    assert edge.acl == inputs["acl"]

    if "properties" in inputs:
        for prop, value in inputs["properties"].items():
            assert prop in edge.properties
            assert edge.properties[prop] == value

    if "system_annotations" in inputs:
        for prop, value in inputs["system_annotations"].items():
            assert prop in edge.system_annotations
            assert edge.system_annotations[prop] == value


def test__poly_node__generates_defaults() -> None:
    node = psqlgraph.PolyNode(label=models.FooBar.get_label())

    assert node.node_id is None
    assert node.acl == []
    assert node.system_annotations == {}
    assert node.properties == {"bar": None}


@pytest.mark.parametrize(
    "inputs",
    (
        pytest.param(
            {
                "label": models.Foo.get_label(),
                "node_id": "f0",
                "acl": ["open"],
                "properties": {"bar": "testing-fun"},
            },
            id="node-with-properties",
        ),
        pytest.param(
            {
                "label": models.Circle1.get_label(),
                "node_id": "c0",
                "acl": ["secret"],
                "system_annotations": {"dummy": "value"},
            },
            id="node-with-system-annotation",
        ),
    ),
)
def test__poly_node__generates_from_input(inputs: dict) -> None:
    """Given a PolyEdge or PolyNode
    When called with an unknown label (i.e. associated with no graph entity.)
    Then a ValueError is raised.
    """
    node = psqlgraph.PolyNode(**inputs)

    assert node.node_id == inputs["node_id"]
    assert node.acl == inputs["acl"]

    if "properties" in inputs:
        for prop, value in inputs["properties"].items():
            assert prop in node.properties
            assert node.properties[prop] == value

    if "system_annotations" in inputs:
        for prop, value in inputs["system_annotations"].items():
            assert prop in node.system_annotations
            assert node.system_annotations[prop] == value


@pytest.mark.parametrize("label", ("mysterious", None))
@pytest.mark.parametrize("entity", (psqlgraph.PolyEdge, psqlgraph.PolyNode))
def test__poly_entity__unknown_label(entity: Callable, label: str | None) -> None:
    """Given a PolyEdge or PolyNode
    When called with an unknown label (i.e. associated with no graph entity.)
    Then a ValueError is raised.
    """
    with pytest.raises(ValueError):
        entity(label=label)
