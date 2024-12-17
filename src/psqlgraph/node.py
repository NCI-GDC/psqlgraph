from psqlgraph.graph import AbstractNode, Node

__all__ = ("AbstractNode", "Node", "PolyNode")


def PolyNode(node_id=None, label=None, acl=None, system_annotations=None, properties=None):
    assert label, "You cannot create a PolyNode without a label."
    Type = Node.get_subclass(label)

    if not Type:
        raise KeyError(f"No node type found with label: {label}")

    return Type(
        node_id=node_id,
        properties=properties or {},
        acl=acl or [],
        system_annotations=system_annotations or {},
        label=label,
    )
