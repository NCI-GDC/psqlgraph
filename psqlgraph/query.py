# from edge import Edge, PsqlEdge, PsqlVoidedEdge
from node import Node
from sqlalchemy.orm import Query, joinedload
from sqlalchemy import or_, not_


class GraphQuery(Query):
    """Query subclass implementing graph specific operations."""

    def _iterable(self, val):
        if hasattr(val, '__iter__'):
            return val
        else:
            return (val,)

    def entity(self):
        return self._entity_zero().type

    # ======== Edges ========
    def with_edge_to_node(self, edge_label, target_node):
        pass

    def with_edge_from_node(self, edge_label, source_node):
        pass

    def src(self, src_id):
        pass

    def dst(self, dst_id):
        pass

    # ====== Nodes ========
    def has_edges(self):
        pass

    def has_no_edges(self):
        pass

    def _path(self, labels, edges, node, reset=False):
        pass

    def path_out(self, labels, reset=False):
        pass

    def path_in(self, labels, reset=False):
        pass

    def path_in_end(self, labels):
        pass

    def path_out_end(self, labels):
        pass

    def path_end(self, labels):
        pass

    def ids_path_end(self, ids, labels):
        pass

    def ids(self, ids):
        entity = self.entity()
        _id = entity._id
        if hasattr(ids, '__iter__'):
            return self.filter(_id.in_(ids))
        else:
            return self.filter(_id == str(ids))

    def neighbors(self):
        pass

    def load_edges(self):
        pass

    def load_neighbors(self):
        pass

    def _flatten_tree(self, tree):
        pass

    @staticmethod
    def _reconstruct_tree(node, nodes, doc, tree, visited):
        pass

    def tree(self, root_id, tree):
        pass

    def path_whole(self, path):
        pass

    def path_linked(self, src_id, path):
        pass

    def not_ids(self, ids):
        pass

    # ======== Labels ========
    def labels(self, labels):
        pass

    def not_labels(self, labels):
        pass

    # ======== Properties ========
    def props(self, props):
        pass

    def not_props(self, props):
        pass

    def prop_in(self, key, values):
        pass

    def prop(self, key, value):
        pass

    def has_props(self, keys):
        pass

    def null_props(self, keys):
        pass

    # ======== System Annotations ========
    def sysan(self, sysans):
        pass

    def not_sysan(self, sysans):
        pass

    def not_sysan_in(self, key, values):
        pass

    def has_sysan(self, keys):
        if isinstance(keys, str):
            keys = [keys]
        for key in keys:
            self = self.filter(self.entity().system_annotations.has_key(key))
        return self
