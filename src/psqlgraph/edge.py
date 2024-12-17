from psqlgraph.graph import AbstractEdge, Edge

__all__ = ("AbstractEdge", "Edge", "PolyEdge")


def PolyEdge(
    src_id=None,
    dst_id=None,
    label=None,
    acl=None,
    system_annotations=None,
    properties=None,
):
    if not label:
        raise AttributeError("You cannot create a PolyEdge without a label.")
    try:
        edge_type_class = Edge.get_subclass(label)
    except Exception as e:
        raise RuntimeError(
            "{}: Unable to determine edge type. If there are more than one "
            "edges with label {}, you need to specify src_label and dst_label"
            "using the PsqlGraphDriver.get_PolyEdge())".format(e, label)
        )

    if not edge_type_class:
        raise KeyError(f"No edge type found with label: {label}")

    return edge_type_class(
        src_id=src_id,
        dst_id=dst_id,
        properties=properties or {},
        acl=acl or [],
        system_annotations=system_annotations or {},
        label=label,
    )
